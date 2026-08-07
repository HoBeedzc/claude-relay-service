const express = require('express')
const apiKeyService = require('../../services/apiKeyService')
const ccrAccountService = require('../../services/account/ccrAccountService')
const claudeAccountService = require('../../services/account/claudeAccountService')
const claudeConsoleAccountService = require('../../services/account/claudeConsoleAccountService')
const geminiAccountService = require('../../services/account/geminiAccountService')
const geminiApiAccountService = require('../../services/account/geminiApiAccountService')
const openaiAccountService = require('../../services/account/openaiAccountService')
const openaiResponsesAccountService = require('../../services/account/openaiResponsesAccountService')
const droidAccountService = require('../../services/account/droidAccountService')
const bedrockAccountService = require('../../services/account/bedrockAccountService')
const redis = require('../../models/redis')
const { authenticateAdmin } = require('../../middleware/auth')
const logger = require('../../utils/logger')
const CostCalculator = require('../../utils/costCalculator')
const pricingService = require('../../services/pricingService')

const router = express.Router()

// 辅助函数：通过索引获取数据，回退到 SCAN
// keyPattern 支持占位符：{id}、{keyId}+{model}、{accountId}+{model}
async function getUsageDataByIndex(indexKey, keyPattern, scanPattern) {
  const members = await redis.client.smembers(indexKey)
  if (members && members.length > 0) {
    const keys = members.map((id) => {
      // 检查是否是 keymodel 格式 (keyId:model)
      if (keyPattern.includes('{keyId}') && keyPattern.includes('{model}')) {
        const [keyId, ...modelParts] = id.split(':')
        const model = modelParts.join(':')
        return keyPattern.replace('{keyId}', keyId).replace('{model}', model)
      }
      // 检查是否是 accountId:model 格式
      if (keyPattern.includes('{accountId}') && keyPattern.includes('{model}')) {
        const [accountId, ...modelParts] = id.split(':')
        const model = modelParts.join(':')
        return keyPattern.replace('{accountId}', accountId).replace('{model}', model)
      }
      return keyPattern.replace('{id}', id)
    })
    const dataList = await redis.batchHgetallChunked(keys)
    const result = []
    keys.forEach((key, i) => {
      if (dataList[i] && Object.keys(dataList[i]).length > 0) {
        result.push({ key, data: dataList[i] })
      }
    })
    return result
  }
  // 索引为空，检查空标记
  const emptyMarker = await redis.client.get(`${indexKey}:empty`)
  if (emptyMarker === '1') {
    return []
  }
  // 回退到 SCAN（兼容历史数据）
  const keys = await redis.scanKeys(scanPattern)
  if (keys.length === 0) {
    // 设置空标记，1小时过期
    await redis.client.setex(`${indexKey}:empty`, 3600, '1')
    return []
  }
  // 建立索引
  const ids = keys.map((k) => {
    if (keyPattern.includes('{keyId}') && keyPattern.includes('{model}')) {
      // keymodel 格式：usage:{keyId}:model:daily:{model}:{date} 或 hourly
      const match =
        k.match(/usage:([^:]+):model:daily:(.+):\d{4}-\d{2}-\d{2}$/) ||
        k.match(/usage:([^:]+):model:hourly:(.+):\d{4}-\d{2}-\d{2}:\d{2}$/)
      if (match) {
        return `${match[1]}:${match[2]}`
      }
    }
    if (keyPattern.includes('{accountId}') && keyPattern.includes('{model}')) {
      // account_usage:model:daily 或 hourly
      const match =
        k.match(/account_usage:model:daily:([^:]+):(.+):\d{4}-\d{2}-\d{2}$/) ||
        k.match(/account_usage:model:hourly:([^:]+):(.+):\d{4}-\d{2}-\d{2}:\d{2}$/)
      if (match) {
        return `${match[1]}:${match[2]}`
      }
    }
    // 通用格式：根据 keyPattern 中 {id} 的位置提取 id
    const patternParts = keyPattern.split(':')
    const idIndex = patternParts.findIndex((p) => p === '{id}')
    if (idIndex !== -1) {
      const parts = k.split(':')
      return parts[idIndex]
    }
    // 回退：提取最后一个 : 前的 id
    const parts = k.split(':')
    return parts[parts.length - 2]
  })
  const validIds = ids.filter(Boolean)
  if (validIds.length > 0) {
    await redis.client.sadd(indexKey, ...validIds)
  }
  const dataList = await redis.batchHgetallChunked(keys)
  const result = []
  keys.forEach((key, i) => {
    if (dataList[i] && Object.keys(dataList[i]).length > 0) {
      result.push({ key, data: dataList[i] })
    }
  })
  return result
}

// ========== 通用辅助函数（收敛文件内重复逻辑） ==========

const toInt = (value) => parseInt(value) || 0

// 从 token 数据构建计费 usage 对象；如果有 ephemeral 5m/1h 拆分数据，添加 cache_creation 子对象以实现精确计费
function buildBillingUsage(data = {}) {
  const usage = {
    input_tokens: toInt(data.inputTokens),
    output_tokens: toInt(data.outputTokens),
    cache_creation_input_tokens: toInt(data.cacheCreateTokens),
    cache_read_input_tokens: toInt(data.cacheReadTokens)
  }
  const eph5m = toInt(data.ephemeral5mTokens)
  const eph1h = toInt(data.ephemeral1hTokens)
  if (eph5m > 0 || eph1h > 0) {
    usage.cache_creation = {
      ephemeral_5m_input_tokens: eph5m,
      ephemeral_1h_input_tokens: eph1h
    }
  }
  return usage
}

// 解析小时粒度的起止时间（默认最近24小时）
function resolveHourRange(startDate, endDate) {
  if (startDate && endDate) {
    return { startTime: new Date(startDate), endTime: new Date(endDate) }
  }
  const endTime = new Date()
  return { startTime: new Date(endTime.getTime() - 24 * 60 * 60 * 1000), endTime }
}

// 构建小时区间元数据列表（hourKey/isoTime/label）
function buildHourInfos(startTime, endTime) {
  const hourInfos = []
  const currentHour = new Date(startTime)
  currentHour.setMinutes(0, 0, 0)

  while (currentHour <= endTime) {
    const tzCurrentHour = redis.getDateInTimezone(currentHour)
    const dateStr = redis.getDateStringInTimezone(currentHour)
    const hour = String(tzCurrentHour.getUTCHours()).padStart(2, '0')
    const month = String(tzCurrentHour.getUTCMonth() + 1).padStart(2, '0')
    const day = String(tzCurrentHour.getUTCDate()).padStart(2, '0')

    hourInfos.push({
      hourKey: `${dateStr}:${hour}`,
      dateStr,
      isoTime: currentHour.toISOString(),
      label: `${month}/${day} ${hour}:00`
    })

    currentHour.setHours(currentHour.getHours() + 1)
  }
  return hourInfos
}

// 构建最近 N 天的日期元数据列表
function buildDayInfos(daysCount) {
  const today = new Date()
  const dayInfos = []
  for (let i = 0; i < daysCount; i++) {
    const date = new Date(today)
    date.setDate(date.getDate() - i)
    dayInfos.push({ dateStr: redis.getDateStringInTimezone(date) })
  }
  return dayInfos
}

// 并行拉取每个时间桶的两组索引数据并合并为 Map
async function fetchUsageAndModelMaps(infos, buildQueries) {
  const usageDataMap = new Map()
  const modelDataMap = new Map()

  const allResults = await Promise.all(
    infos.map(async (info) => {
      const { usage, model } = buildQueries(info)
      const [usageResults, modelResults] = await Promise.all([
        getUsageDataByIndex(...usage),
        getUsageDataByIndex(...model)
      ])
      return { usageResults, modelResults }
    })
  )

  allResults.forEach(({ usageResults, modelResults }) => {
    usageResults.forEach(({ key, data }) => usageDataMap.set(key, data))
    modelResults.forEach(({ key, data }) => modelDataMap.set(key, data))
  })
  return { usageDataMap, modelDataMap }
}

// 按正则捕获组把 key 分组到 Map<mapKey, key[]>
function groupKeysByRegex(keys, regex, buildMapKey = (match) => match[1]) {
  const grouped = new Map()
  for (const key of keys) {
    const match = key.match(regex)
    if (match) {
      const mapKey = buildMapKey(match)
      if (!grouped.has(mapKey)) {
        grouped.set(mapKey, [])
      }
      grouped.get(mapKey).push(key)
    }
  }
  return grouped
}

// 构建某天的时区日期键与展示标签
function buildDayLabelInfo(date) {
  const tzDate = redis.getDateInTimezone(date)
  const dateKey = redis.getDateStringInTimezone(date)
  const month = String(tzDate.getUTCMonth() + 1).padStart(2, '0')
  const day = String(tzDate.getUTCDate()).padStart(2, '0')
  return { dateKey, label: `${month}/${day}` }
}

// 创建使用历史累计器（记录 history、总量与峰值日）
function createUsageHistoryTracker() {
  const state = {
    history: [],
    totalCost: 0,
    totalRequests: 0,
    totalTokens: 0,
    highestCostDay: null,
    highestRequestDay: null
  }
  return {
    state,
    add({ dateKey, label, cost, requests, tokens }) {
      state.totalCost += cost
      state.totalRequests += requests
      state.totalTokens += tokens

      if (!state.highestCostDay || cost > state.highestCostDay.cost) {
        state.highestCostDay = {
          date: dateKey,
          label,
          cost,
          formattedCost: CostCalculator.formatCost(cost)
        }
      }
      if (!state.highestRequestDay || requests > state.highestRequestDay.requests) {
        state.highestRequestDay = { date: dateKey, label, requests }
      }

      state.history.push({
        date: dateKey,
        label,
        cost,
        formattedCost: CostCalculator.formatCost(cost),
        requests,
        tokens
      })
    }
  }
}

// 构建使用历史 summary（日均值按创建时间截断，至少1天避免除零）
function buildUsageHistorySummary({ daysCount, createdAt, state }) {
  let actualDaysForAvg = daysCount
  if (createdAt) {
    const diffDays = Math.ceil(Math.abs(new Date() - createdAt) / (1000 * 60 * 60 * 24))
    actualDaysForAvg = Math.max(Math.min(diffDays, daysCount), 1)
  }

  const { history, totalCost, totalRequests, totalTokens, highestCostDay, highestRequestDay } =
    state
  const avgDailyCost = actualDaysForAvg > 0 ? totalCost / actualDaysForAvg : 0
  const todayData = history.length > 0 ? history[history.length - 1] : null

  return {
    days: daysCount,
    actualDaysUsed: actualDaysForAvg,
    totalCost,
    totalCostFormatted: CostCalculator.formatCost(totalCost),
    totalRequests,
    totalTokens,
    avgDailyCost,
    avgDailyCostFormatted: CostCalculator.formatCost(avgDailyCost),
    avgDailyRequests: actualDaysForAvg > 0 ? totalRequests / actualDaysForAvg : 0,
    avgDailyTokens: actualDaysForAvg > 0 ? totalTokens / actualDaysForAvg : 0,
    today: todayData
      ? {
          date: todayData.date,
          cost: todayData.cost,
          costFormatted: todayData.formattedCost,
          requests: todayData.requests,
          tokens: todayData.tokens
        }
      : null,
    highestCostDay,
    highestRequestDay
  }
}

// 聚合单个时间桶的模型/总量数据（usage-trend 小时与天粒度共用）
function aggregateTrendBucket({
  modelKeys,
  usageKeys,
  modelDataMap,
  usageDataMap,
  modelKeyRegex,
  requireUsageKeys
}) {
  const bucket = {
    inputTokens: 0,
    outputTokens: 0,
    requests: 0,
    cacheCreateTokens: 0,
    cacheReadTokens: 0,
    cost: 0
  }

  // 处理模型级别数据
  for (const modelKey of modelKeys) {
    const modelMatch = modelKey.match(modelKeyRegex)
    if (!modelMatch) {
      continue
    }
    const model = modelMatch[1]
    const data = modelDataMap.get(modelKey)
    if (!data || Object.keys(data).length === 0) {
      continue
    }

    const usage = buildBillingUsage(data)
    bucket.inputTokens += usage.input_tokens
    bucket.outputTokens += usage.output_tokens
    bucket.cacheCreateTokens += usage.cache_creation_input_tokens
    bucket.cacheReadTokens += usage.cache_read_input_tokens
    bucket.requests += toInt(data.requests)
    bucket.cost += CostCalculator.calculateCost(usage, model).costs.total
  }

  // 如果没有模型级别的数据，回退到 API Key 级别的数据
  if (modelKeys.length === 0 && (!requireUsageKeys || usageKeys.length > 0)) {
    let eph5m = 0
    let eph1h = 0
    for (const key of usageKeys) {
      const data = usageDataMap.get(key)
      if (data) {
        bucket.inputTokens += toInt(data.inputTokens)
        bucket.outputTokens += toInt(data.outputTokens)
        bucket.requests += toInt(data.requests)
        bucket.cacheCreateTokens += toInt(data.cacheCreateTokens)
        bucket.cacheReadTokens += toInt(data.cacheReadTokens)
        eph5m += toInt(data.ephemeral5mTokens)
        eph1h += toInt(data.ephemeral1hTokens)
      }
    }

    const usage = buildBillingUsage({
      inputTokens: bucket.inputTokens,
      outputTokens: bucket.outputTokens,
      cacheCreateTokens: bucket.cacheCreateTokens,
      cacheReadTokens: bucket.cacheReadTokens,
      ephemeral5mTokens: eph5m,
      ephemeral1hTokens: eph1h
    })
    bucket.cost = CostCalculator.calculateCost(usage, 'unknown').costs.total
  }

  return bucket
}

// 将账号列表映射为趋势图使用的精简结构
const mapTrendAccounts = (accounts, platform, buildName) =>
  accounts.map((account) => {
    const id = String(account.id || '')
    const shortId = id ? id.slice(0, 8) : '未知'
    return { id, name: buildName(account, shortId), platform }
  })

// 处理单个时间桶内各账号的用量与费用（account-usage-trend 小时与天粒度共用）
function buildAccountBucketStats({
  usageKeys,
  usageDataMap,
  modelKeysMap,
  modelDataMap,
  bucketKey,
  usageKeyRegex,
  accountIdSet,
  accountMap,
  fallbackModel,
  accountCostTotals
}) {
  const accounts = {}

  for (const key of usageKeys) {
    const match = key.match(usageKeyRegex)
    if (!match) {
      continue
    }

    const accountId = match[1]
    if (!accountIdSet.has(accountId)) {
      continue
    }

    const data = usageDataMap.get(key)
    if (!data) {
      continue
    }

    const inputTokens = toInt(data.inputTokens)
    const outputTokens = toInt(data.outputTokens)
    const cacheCreateTokens = toInt(data.cacheCreateTokens)
    const cacheReadTokens = toInt(data.cacheReadTokens)
    const allTokens =
      toInt(data.allTokens) || inputTokens + outputTokens + cacheCreateTokens + cacheReadTokens
    const requests = toInt(data.requests)

    // 计算模型费用（从预加载的数据中）
    let cost = 0
    const modelKeys = modelKeysMap.get(`${accountId}:${bucketKey}`) || []
    for (const modelKey of modelKeys) {
      const modelData = modelDataMap.get(modelKey)
      if (!modelData) {
        continue
      }

      const parts = modelKey.split(':')
      if (parts.length < 5) {
        continue
      }

      const modelName = parts[4]
      cost += CostCalculator.calculateCost(buildBillingUsage(modelData), modelName).costs.total
    }

    if (cost === 0 && allTokens > 0) {
      cost = CostCalculator.calculateCost(buildBillingUsage(data), fallbackModel).costs.total
    }

    const accountInfo = accountMap.get(accountId)
    accounts[accountId] = {
      name: accountInfo ? accountInfo.name : `账号 ${accountId.slice(0, 8)}`,
      cost,
      formattedCost: CostCalculator.formatCost(cost),
      requests
    }

    accountCostTotals.set(accountId, (accountCostTotals.get(accountId) || 0) + cost)
  }

  return accounts
}

// 处理单个时间桶内各 API Key 的用量与费用（api-keys-usage-trend 小时与天粒度共用）
function buildApiKeyBucketStats({
  usageKeys,
  modelKeys,
  usageDataMap,
  modelDataMap,
  usageKeyRegex,
  modelKeyRegex,
  apiKeyMap
}) {
  // 处理 usage 数据
  const apiKeyDataMap = new Map()
  for (const key of usageKeys) {
    const match = key.match(usageKeyRegex)
    if (!match) {
      continue
    }

    const apiKeyId = match[1]
    const data = usageDataMap.get(key)
    if (!data || !apiKeyMap.has(apiKeyId)) {
      continue
    }

    const inputTokens = toInt(data.inputTokens)
    const outputTokens = toInt(data.outputTokens)
    const cacheCreateTokens = toInt(data.cacheCreateTokens)
    const cacheReadTokens = toInt(data.cacheReadTokens)

    apiKeyDataMap.set(apiKeyId, {
      name: apiKeyMap.get(apiKeyId).name,
      tokens: inputTokens + outputTokens + cacheCreateTokens + cacheReadTokens,
      requests: toInt(data.requests),
      inputTokens,
      outputTokens,
      cacheCreateTokens,
      cacheReadTokens,
      ephemeral5mTokens: toInt(data.ephemeral5mTokens),
      ephemeral1hTokens: toInt(data.ephemeral1hTokens)
    })
  }

  // 处理 model 数据计算费用
  const apiKeyCostMap = new Map()
  for (const modelKey of modelKeys) {
    const match = modelKey.match(modelKeyRegex)
    if (!match) {
      continue
    }

    const apiKeyId = match[1]
    const model = match[2]
    const modelData = modelDataMap.get(modelKey)
    if (!modelData || !apiKeyDataMap.has(apiKeyId)) {
      continue
    }

    // 优先使用已存储的费用
    const hasStoredCost = 'realCostMicro' in modelData || 'ratedCostMicro' in modelData
    let modelCost = 0

    if (hasStoredCost) {
      modelCost = toInt(modelData.ratedCostMicro) / 1000000
    } else {
      // Legacy fallback：旧数据没有存储费用，从 token 重算
      modelCost = CostCalculator.calculateCost(buildBillingUsage(modelData), model).costs.total
    }

    apiKeyCostMap.set(apiKeyId, (apiKeyCostMap.get(apiKeyId) || 0) + modelCost)
  }

  // 组合数据
  const bucketApiKeys = {}
  for (const [apiKeyId, data] of apiKeyDataMap) {
    let cost = apiKeyCostMap.get(apiKeyId) || 0
    let formattedCost = CostCalculator.formatCost(cost)

    // 降级方案
    if (cost === 0 && data.tokens > 0) {
      const fallbackResult = CostCalculator.calculateCost(
        buildBillingUsage(data),
        'claude-3-5-sonnet-20241022'
      )
      cost = fallbackResult.costs.total
      formattedCost = fallbackResult.formatted.total
    }

    bucketApiKeys[apiKeyId] = {
      name: data.name,
      tokens: data.tokens,
      requests: data.requests,
      cost,
      formattedCost
    }
  }

  return bucketApiKeys
}

// 累加模型统计（model-stats 自定义与预设期间共用）
function accumulateModelStats(modelStatsMap, model, data) {
  if (!modelStatsMap.has(model)) {
    modelStatsMap.set(model, {
      requests: 0,
      inputTokens: 0,
      outputTokens: 0,
      cacheCreateTokens: 0,
      cacheReadTokens: 0,
      ephemeral5mTokens: 0,
      ephemeral1hTokens: 0,
      allTokens: 0,
      realCostMicro: 0,
      ratedCostMicro: 0,
      hasStoredCost: false
    })
  }
  const stats = modelStatsMap.get(model)
  stats.requests += toInt(data.requests)
  stats.inputTokens += toInt(data.inputTokens)
  stats.outputTokens += toInt(data.outputTokens)
  stats.cacheCreateTokens += toInt(data.cacheCreateTokens)
  stats.cacheReadTokens += toInt(data.cacheReadTokens)
  stats.ephemeral5mTokens += toInt(data.ephemeral5mTokens)
  stats.ephemeral1hTokens += toInt(data.ephemeral1hTokens)
  stats.allTokens += toInt(data.allTokens)
  if ('realCostMicro' in data || 'ratedCostMicro' in data) {
    stats.realCostMicro += toInt(data.realCostMicro)
    stats.ratedCostMicro += toInt(data.ratedCostMicro)
    stats.hasStoredCost = true
  }
}

// 累加模型 token 用量（usage-costs 共用）
function accumulateModelUsage(modelUsageMap, model, data) {
  if (!modelUsageMap.has(model)) {
    modelUsageMap.set(model, {
      inputTokens: 0,
      outputTokens: 0,
      cacheCreateTokens: 0,
      cacheReadTokens: 0,
      ephemeral5mTokens: 0,
      ephemeral1hTokens: 0
    })
  }
  const modelUsage = modelUsageMap.get(model)
  modelUsage.inputTokens += toInt(data.inputTokens)
  modelUsage.outputTokens += toInt(data.outputTokens)
  modelUsage.cacheCreateTokens += toInt(data.cacheCreateTokens)
  modelUsage.cacheReadTokens += toInt(data.cacheReadTokens)
  modelUsage.ephemeral5mTokens += toInt(data.ephemeral5mTokens)
  modelUsage.ephemeral1hTokens += toInt(data.ephemeral1hTokens)
}

// 累加单模型费用到 totalCosts
function addCostResult(totalCosts, costResult) {
  totalCosts.inputCost += costResult.costs.input
  totalCosts.outputCost += costResult.costs.output
  totalCosts.cacheCreateCost += costResult.costs.cacheWrite
  totalCosts.cacheReadCost += costResult.costs.cacheRead
  totalCosts.totalCost += costResult.costs.total
}

// 格式化 totalCosts 响应结构
function formatTotalCosts(totalCosts) {
  return {
    ...totalCosts,
    formatted: {
      inputCost: CostCalculator.formatCost(totalCosts.inputCost),
      outputCost: CostCalculator.formatCost(totalCosts.outputCost),
      cacheCreateCost: CostCalculator.formatCost(totalCosts.cacheCreateCost),
      cacheReadCost: CostCalculator.formatCost(totalCosts.cacheReadCost),
      totalCost: CostCalculator.formatCost(totalCosts.totalCost)
    }
  }
}

// 汇总 modelUsageMap 到 totalCosts 与 modelCosts（7天/全部期间共用）
function summarizeModelUsageCosts(modelUsageMap, totalCosts, modelCosts, logSuffix = '') {
  for (const [model, usage] of modelUsageMap) {
    const usageData = buildBillingUsage(usage)
    const costResult = CostCalculator.calculateCost(usageData, model)
    addCostResult(totalCosts, costResult)

    logger.info(
      `💰 Model ${model}${logSuffix}: ${
        usage.inputTokens + usage.outputTokens + usage.cacheCreateTokens + usage.cacheReadTokens
      } tokens, cost: ${costResult.formatted.total}`
    )

    // 记录模型费用（汇总数据没有请求数统计）
    modelCosts[model] = {
      model,
      requests: 0,
      usage: usageData,
      costs: costResult.costs,
      formatted: costResult.formatted,
      usingDynamicPricing: costResult.usingDynamicPricing
    }
  }
}

// 将请求记录转换为计费 usage 对象（usage-records 共用）
const recordToUsageObject = (record) => {
  const usage = {
    input_tokens: record.inputTokens || 0,
    output_tokens: record.outputTokens || 0,
    cache_creation_input_tokens: record.cacheCreateTokens || 0,
    cache_read_input_tokens: record.cacheReadTokens || 0,
    cache_creation: record.cacheCreation || record.cache_creation || null
  }
  // 如果没有 cache_creation 但有独立存储的 ephemeral 字段，构建子对象
  if (!usage.cache_creation) {
    const eph5m = toInt(record.ephemeral5mTokens)
    const eph1h = toInt(record.ephemeral1hTokens)
    if (eph5m > 0 || eph1h > 0) {
      usage.cache_creation = {
        ephemeral_5m_input_tokens: eph5m,
        ephemeral_1h_input_tokens: eph1h
      }
    }
  }
  return usage
}

// 解析请求记录查询公共参数（分页/排序/时间范围）
function parseRecordQuery(query) {
  const pageNumber = Math.max(parseInt(query.page, 10) || 1, 1)
  const pageSizeNumber = Math.min(Math.max(parseInt(query.pageSize, 10) || 50, 1), 200)
  const normalizedSortOrder = query.sortOrder === 'asc' ? 'asc' : 'desc'
  const startTime = query.startDate ? new Date(query.startDate) : null
  const endTime = query.endDate ? new Date(query.endDate) : null
  const invalidRange =
    (query.startDate && Number.isNaN(startTime?.getTime())) ||
    (query.endDate && Number.isNaN(endTime?.getTime()))
  return { pageNumber, pageSizeNumber, normalizedSortOrder, startTime, endTime, invalidRange }
}

// 判断记录时间是否在范围内
const isRecordWithinRange = (record, startTime, endTime) => {
  if (!record.timestamp) {
    return false
  }
  const ts = new Date(record.timestamp)
  if (Number.isNaN(ts.getTime())) {
    return false
  }
  if (startTime && ts < startTime) {
    return false
  }
  if (endTime && ts > endTime) {
    return false
  }
  return true
}

// 按时间排序请求记录（原地排序）
function sortRecordsByTimestamp(records, order) {
  records.sort((a, b) => {
    const aTime = new Date(a.timestamp).getTime()
    const bTime = new Date(b.timestamp).getTime()
    if (Number.isNaN(aTime) || Number.isNaN(bTime)) {
      return 0
    }
    return order === 'asc' ? aTime - bTime : bTime - aTime
  })
}

// 请求记录分页切片
function paginateRecords(records, pageNumber, pageSizeNumber) {
  const totalRecords = records.length
  const totalPages = totalRecords > 0 ? Math.ceil(totalRecords / pageSizeNumber) : 0
  const safePage = totalPages > 0 ? Math.min(pageNumber, totalPages) : 1
  const startIndex = (safePage - 1) * pageSizeNumber
  const pageRecords =
    totalRecords === 0 ? [] : records.slice(startIndex, startIndex + pageSizeNumber)
  return {
    pageRecords,
    pagination: {
      currentPage: safePage,
      pageSize: pageSizeNumber,
      totalRecords,
      totalPages,
      hasNextPage: totalPages > 0 && safePage < totalPages,
      hasPreviousPage: totalPages > 0 && safePage > 1
    }
  }
}

// 计算单条记录的费用与 token 汇总
function computeRecordCost(record) {
  const usage = recordToUsageObject(record)
  const costModel = record.actualModel || record.model || 'unknown'
  const costData = CostCalculator.calculateCost(usage, costModel)
  const computedCost = typeof record.cost === 'number' ? record.cost : costData?.costs?.total || 0
  const realCost =
    typeof record.realCost === 'number' ? record.realCost : costData?.costs?.total || 0
  const totalTokens =
    record.totalTokens ||
    usage.input_tokens +
      usage.output_tokens +
      usage.cache_creation_input_tokens +
      usage.cache_read_input_tokens
  return { usage, costData, computedCost, realCost, totalTokens }
}

// 累加单条记录到 summary
function addRecordToSummary(summary, usage, totalTokens, computedCost) {
  summary.totalRequests += 1
  summary.inputTokens += usage.input_tokens
  summary.outputTokens += usage.output_tokens
  summary.cacheCreateTokens += usage.cache_creation_input_tokens
  summary.cacheReadTokens += usage.cache_read_input_tokens
  summary.totalTokens += totalTokens
  summary.totalCost += computedCost
}

// 构建记录的 costBreakdown 字段
const buildCostBreakdown = (record, costData, computedCost) =>
  record.realCostBreakdown ||
  record.costBreakdown || {
    input: costData?.costs?.input || 0,
    output: costData?.costs?.output || 0,
    cacheCreate: costData?.costs?.cacheWrite || 0,
    cacheRead: costData?.costs?.cacheRead || 0,
    total: costData?.costs?.total || computedCost
  }

// summary 附加总费用与均值字段
const finalizeRecordSummary = (summary) => ({
  ...summary,
  totalCost: Number(summary.totalCost.toFixed(6)),
  avgCost:
    summary.totalRequests > 0 ? Number((summary.totalCost / summary.totalRequests).toFixed(6)) : 0
})

const accountTypeNames = {
  claude: 'Claude官方',
  'claude-official': 'Claude官方',
  'claude-console': 'Claude Console',
  ccr: 'Claude Console Relay',
  openai: 'OpenAI',
  'openai-responses': 'OpenAI Responses',
  gemini: 'Gemini',
  'gemini-api': 'Gemini API',
  droid: 'Droid',
  bedrock: 'AWS Bedrock',
  unknown: '未知渠道'
}

const resolveAccountByPlatform = async (accountId, platform) => {
  const serviceMap = {
    claude: claudeAccountService,
    'claude-console': claudeConsoleAccountService,
    gemini: geminiAccountService,
    'gemini-api': geminiApiAccountService,
    openai: openaiAccountService,
    'openai-responses': openaiResponsesAccountService,
    droid: droidAccountService,
    ccr: ccrAccountService,
    bedrock: bedrockAccountService
  }

  if (platform && serviceMap[platform]) {
    try {
      const account = await serviceMap[platform].getAccount(accountId)
      if (account) {
        return { ...account, platform }
      }
    } catch (error) {
      logger.debug(`⚠️ Failed to get account ${accountId} from ${platform}: ${error.message}`)
    }
  }

  for (const [platformName, service] of Object.entries(serviceMap)) {
    try {
      const account = await service.getAccount(accountId)
      if (account) {
        return { ...account, platform: platformName }
      }
    } catch (error) {
      logger.debug(`⚠️ Failed to get account ${accountId} from ${platformName}: ${error.message}`)
    }
  }

  return null
}

const getApiKeyName = async (keyId) => {
  try {
    const keyData = await redis.getApiKey(keyId)
    return keyData?.name || keyData?.label || keyId
  } catch (error) {
    logger.debug(`⚠️ Failed to get API key name for ${keyId}: ${error.message}`)
    return keyId
  }
}

// 📊 账户使用统计

// 获取所有账户的使用统计
router.get('/accounts/usage-stats', authenticateAdmin, async (req, res) => {
  try {
    const accountsStats = await redis.getAllAccountsUsageStats()

    return res.json({
      success: true,
      data: accountsStats,
      summary: {
        totalAccounts: accountsStats.length,
        activeToday: accountsStats.filter((account) => account.daily.requests > 0).length,
        totalDailyTokens: accountsStats.reduce(
          (sum, account) => sum + (account.daily.allTokens || 0),
          0
        ),
        totalDailyRequests: accountsStats.reduce(
          (sum, account) => sum + (account.daily.requests || 0),
          0
        )
      },
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    logger.error('❌ Failed to get accounts usage stats:', error)
    return res.status(500).json({
      success: false,
      error: 'Failed to get accounts usage stats',
      message: error.message
    })
  }
})

// 获取单个账户的使用统计
router.get('/accounts/:accountId/usage-stats', authenticateAdmin, async (req, res) => {
  try {
    const { accountId } = req.params
    const accountStats = await redis.getAccountUsageStats(accountId)

    // 获取账户基本信息
    const accountData = await claudeAccountService.getAccount(accountId)
    if (!accountData) {
      return res.status(404).json({
        success: false,
        error: 'Account not found'
      })
    }

    return res.json({
      success: true,
      data: {
        ...accountStats,
        accountInfo: {
          name: accountData.name,
          email: accountData.email,
          status: accountData.status,
          isActive: accountData.isActive,
          createdAt: accountData.createdAt
        }
      },
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    logger.error('❌ Failed to get account usage stats:', error)
    return res.status(500).json({
      success: false,
      error: 'Failed to get account usage stats',
      message: error.message
    })
  }
})

// 获取账号近30天使用历史
router.get('/accounts/:accountId/usage-history', authenticateAdmin, async (req, res) => {
  try {
    const { accountId } = req.params
    const { platform = 'claude', days = 30 } = req.query

    const allowedPlatforms = [
      'claude',
      'claude-console',
      'openai',
      'openai-responses',
      'gemini',
      'gemini-api',
      'droid',
      'bedrock'
    ]
    if (!allowedPlatforms.includes(platform)) {
      return res.status(400).json({
        success: false,
        error: 'Unsupported account platform'
      })
    }

    const accountTypeMap = {
      openai: 'openai',
      'openai-responses': 'openai-responses',
      'gemini-api': 'gemini-api',
      droid: 'droid',
      bedrock: 'bedrock'
    }

    const fallbackModelMap = {
      claude: 'claude-3-5-sonnet-20241022',
      'claude-console': 'claude-3-5-sonnet-20241022',
      openai: 'gpt-4o-mini-2024-07-18',
      'openai-responses': 'gpt-4o-mini-2024-07-18',
      gemini: 'gemini-1.5-flash',
      'gemini-api': 'gemini-2.0-flash',
      droid: 'unknown',
      bedrock: 'us.anthropic.claude-3-5-sonnet-20241022-v2:0'
    }

    // 获取账户信息以获取创建时间
    let accountData = null
    let accountCreatedAt = null

    try {
      switch (platform) {
        case 'claude':
          accountData = await claudeAccountService.getAccount(accountId)
          break
        case 'claude-console':
          accountData = await claudeConsoleAccountService.getAccount(accountId)
          break
        case 'openai':
          accountData = await openaiAccountService.getAccount(accountId)
          break
        case 'openai-responses':
          accountData = await openaiResponsesAccountService.getAccount(accountId)
          break
        case 'gemini':
          accountData = await geminiAccountService.getAccount(accountId)
          break
        case 'gemini-api': {
          accountData = await geminiApiAccountService.getAccount(accountId)
          break
        }
        case 'droid':
          accountData = await droidAccountService.getAccount(accountId)
          break
        case 'bedrock': {
          const result = await bedrockAccountService.getAccount(accountId)
          accountData = result?.success ? result.data : null
          break
        }
      }

      if (accountData && accountData.createdAt) {
        accountCreatedAt = new Date(accountData.createdAt)
      }
    } catch (error) {
      logger.warn(`Failed to get account data for avgDailyCost calculation: ${error.message}`)
    }

    const fallbackModel = fallbackModelMap[platform] || 'unknown'
    const daysCount = Math.min(Math.max(parseInt(days, 10) || 30, 1), 60)

    // 获取概览统计数据
    const accountUsageStats = await redis.getAccountUsageStats(
      accountId,
      accountTypeMap[platform] || null
    )

    const tracker = createUsageHistoryTracker()

    const sumModelCostsForDay = async (dateKey) => {
      const modelPattern = `account_usage:model:daily:${accountId}:*:${dateKey}`
      const modelResults = await redis.scanAndGetAllChunked(modelPattern)
      let summedCost = 0

      for (const { key: modelKey, data: modelData } of modelResults) {
        const modelParts = modelKey.split(':')
        const modelName = modelParts[4] || 'unknown'
        if (!modelData || Object.keys(modelData).length === 0) {
          continue
        }

        summedCost += CostCalculator.calculateCost(buildBillingUsage(modelData), modelName).costs
          .total
      }

      return summedCost
    }

    const today = new Date()

    for (let offset = daysCount - 1; offset >= 0; offset--) {
      const date = new Date(today)
      date.setDate(date.getDate() - offset)
      const { dateKey, label } = buildDayLabelInfo(date)

      const client = redis.getClientSafe()
      const dailyKey = `account_usage:daily:${accountId}:${dateKey}`
      const dailyData = await client.hgetall(dailyKey)

      const inputTokens = toInt(dailyData?.inputTokens)
      const outputTokens = toInt(dailyData?.outputTokens)
      const cacheCreateTokens = toInt(dailyData?.cacheCreateTokens)
      const cacheReadTokens = toInt(dailyData?.cacheReadTokens)
      const allTokens =
        toInt(dailyData?.allTokens) ||
        inputTokens + outputTokens + cacheCreateTokens + cacheReadTokens
      const requests = toInt(dailyData?.requests)

      let cost = await sumModelCostsForDay(dateKey)

      if (cost === 0 && allTokens > 0) {
        cost = CostCalculator.calculateCost(buildBillingUsage(dailyData || {}), fallbackModel).costs
          .total
      }

      const normalizedCost = Math.round(cost * 1_000_000) / 1_000_000
      tracker.add({ dateKey, label, cost: normalizedCost, requests, tokens: allTokens })
    }

    return res.json({
      success: true,
      data: {
        history: tracker.state.history,
        summary: {
          ...buildUsageHistorySummary({
            daysCount,
            createdAt: accountCreatedAt,
            state: tracker.state
          }),
          accountCreatedAt: accountCreatedAt ? accountCreatedAt.toISOString() : null
        },
        overview: accountUsageStats,
        generatedAt: new Date().toISOString()
      }
    })
  } catch (error) {
    logger.error('❌ Failed to get account usage history:', error)
    return res.status(500).json({
      success: false,
      error: 'Failed to get account usage history',
      message: error.message
    })
  }
})

// 获取单个 API Key 最近使用历史（用于详情弹窗走势图）
router.get('/api-keys/:keyId/usage-history', authenticateAdmin, async (req, res) => {
  try {
    const { keyId } = req.params
    const { days = 30 } = req.query

    const keyData = await redis.getApiKey(keyId)
    if (!keyData || Object.keys(keyData).length === 0) {
      return res.status(404).json({
        success: false,
        error: 'API key not found'
      })
    }

    const daysCount = Math.min(Math.max(parseInt(days, 10) || 30, 1), 60)
    const keyCreatedAt = keyData.createdAt ? new Date(keyData.createdAt) : null
    const keyUsageStats = await redis.getUsageStats(keyId)

    const tracker = createUsageHistoryTracker()
    const today = new Date()

    for (let offset = daysCount - 1; offset >= 0; offset--) {
      const date = new Date(today)
      date.setDate(date.getDate() - offset)
      const { dateKey, label } = buildDayLabelInfo(date)

      const dailyKey = `usage:daily:${keyId}:${dateKey}`
      const dailyData = await redis.client.hgetall(dailyKey)
      const modelResults = await redis.scanAndGetAllChunked(
        `usage:${keyId}:model:daily:*:${dateKey}`
      )

      let inputTokens = 0
      let outputTokens = 0
      let cacheCreateTokens = 0
      let cacheReadTokens = 0
      let requests = 0
      let cost = 0

      if (modelResults.length > 0) {
        for (const { key: modelKey, data: modelData } of modelResults) {
          const match = modelKey.match(/^usage:[^:]+:model:daily:(.+):\d{4}-\d{2}-\d{2}$/)
          const model = match ? match[1] : 'unknown'

          const modelInputTokens =
            parseInt(modelData.totalInputTokens) || parseInt(modelData.inputTokens) || 0
          const modelOutputTokens =
            parseInt(modelData.totalOutputTokens) || parseInt(modelData.outputTokens) || 0
          const modelCacheCreateTokens =
            parseInt(modelData.totalCacheCreateTokens) || parseInt(modelData.cacheCreateTokens) || 0
          const modelCacheReadTokens =
            parseInt(modelData.totalCacheReadTokens) || parseInt(modelData.cacheReadTokens) || 0
          const modelRequests =
            parseInt(modelData.totalRequests) || parseInt(modelData.requests) || 0

          inputTokens += modelInputTokens
          outputTokens += modelOutputTokens
          cacheCreateTokens += modelCacheCreateTokens
          cacheReadTokens += modelCacheReadTokens
          requests += modelRequests

          if ('ratedCostMicro' in modelData || 'realCostMicro' in modelData) {
            cost += (parseInt(modelData.ratedCostMicro) || 0) / 1000000
            continue
          }

          const usage = buildBillingUsage({
            inputTokens: modelInputTokens,
            outputTokens: modelOutputTokens,
            cacheCreateTokens: modelCacheCreateTokens,
            cacheReadTokens: modelCacheReadTokens,
            ephemeral5mTokens: modelData.ephemeral5mTokens,
            ephemeral1hTokens: modelData.ephemeral1hTokens
          })

          const costResult = CostCalculator.calculateCost(usage, model)
          cost += costResult.costs.total
        }
      } else {
        inputTokens = parseInt(dailyData.totalInputTokens) || parseInt(dailyData.inputTokens) || 0
        outputTokens =
          parseInt(dailyData.totalOutputTokens) || parseInt(dailyData.outputTokens) || 0
        cacheCreateTokens =
          parseInt(dailyData.totalCacheCreateTokens) || parseInt(dailyData.cacheCreateTokens) || 0
        cacheReadTokens =
          parseInt(dailyData.totalCacheReadTokens) || parseInt(dailyData.cacheReadTokens) || 0
        requests = parseInt(dailyData.totalRequests) || parseInt(dailyData.requests) || 0

        const allTokens =
          parseInt(dailyData.totalAllTokens) ||
          parseInt(dailyData.allTokens) ||
          inputTokens + outputTokens + cacheCreateTokens + cacheReadTokens

        if (allTokens > 0) {
          const usage = buildBillingUsage({
            inputTokens,
            outputTokens,
            cacheCreateTokens,
            cacheReadTokens,
            ephemeral5mTokens: dailyData.ephemeral5mTokens,
            ephemeral1hTokens: dailyData.ephemeral1hTokens
          })

          const costResult = CostCalculator.calculateCost(usage, 'unknown')
          cost = costResult.costs.total
        }
      }

      const tokens = inputTokens + outputTokens + cacheCreateTokens + cacheReadTokens
      const normalizedCost = Math.round(cost * 1000000) / 1000000
      tracker.add({ dateKey, label, cost: normalizedCost, requests, tokens })
    }

    return res.json({
      success: true,
      data: {
        history: tracker.state.history,
        summary: {
          ...buildUsageHistorySummary({ daysCount, createdAt: keyCreatedAt, state: tracker.state }),
          keyCreatedAt: keyCreatedAt ? keyCreatedAt.toISOString() : null
        },
        overview: keyUsageStats,
        generatedAt: new Date().toISOString()
      }
    })
  } catch (error) {
    logger.error('❌ Failed to get API key usage history:', error)
    return res.status(500).json({
      success: false,
      error: 'Failed to get API key usage history',
      message: error.message
    })
  }
})

// 📊 使用趋势和成本分析

// 获取使用趋势数据
router.get('/usage-trend', authenticateAdmin, async (req, res) => {
  try {
    const { days = 7, granularity = 'day', startDate, endDate } = req.query

    const trendData = []

    if (granularity === 'hour') {
      // 小时粒度统计
      const { startTime, endTime } = resolveHourRange(startDate, endDate)

      // 确保时间范围不超过24小时
      const timeDiff = endTime - startTime
      if (timeDiff > 24 * 60 * 60 * 1000) {
        return res.status(400).json({
          error: '小时粒度查询时间范围不能超过24小时'
        })
      }

      // 收集所有小时的元数据，并行按小时批量查询索引数据
      const hourInfos = buildHourInfos(startTime, endTime)
      const { usageDataMap, modelDataMap } = await fetchUsageAndModelMaps(
        hourInfos,
        (hourInfo) => ({
          usage: [
            `usage:hourly:index:${hourInfo.hourKey}`,
            `usage:hourly:{id}:${hourInfo.hourKey}`,
            `usage:hourly:*:${hourInfo.hourKey}`
          ],
          model: [
            `usage:model:hourly:index:${hourInfo.hourKey}`,
            `usage:model:hourly:{id}:${hourInfo.hourKey}`,
            `usage:model:hourly:*:${hourInfo.hourKey}`
          ]
        })
      )

      // 按 hourKey 分组
      const modelKeysByHour = groupKeysByRegex(
        modelDataMap.keys(),
        /usage:model:hourly:.+?:(\d{4}-\d{2}-\d{2}:\d{2})/
      )
      const usageKeysByHour = groupKeysByRegex(
        usageDataMap.keys(),
        /usage:hourly:.+?:(\d{4}-\d{2}-\d{2}:\d{2})/
      )

      // 处理每个小时的数据
      for (const hourInfo of hourInfos) {
        const bucket = aggregateTrendBucket({
          modelKeys: modelKeysByHour.get(hourInfo.hourKey) || [],
          usageKeys: usageKeysByHour.get(hourInfo.hourKey) || [],
          modelDataMap,
          usageDataMap,
          modelKeyRegex: /usage:model:hourly:(.+?):\d{4}-\d{2}-\d{2}:\d{2}/,
          requireUsageKeys: false
        })

        trendData.push({
          hour: hourInfo.isoTime,
          label: hourInfo.label,
          inputTokens: bucket.inputTokens,
          outputTokens: bucket.outputTokens,
          requests: bucket.requests,
          cacheCreateTokens: bucket.cacheCreateTokens,
          cacheReadTokens: bucket.cacheReadTokens,
          totalTokens:
            bucket.inputTokens +
            bucket.outputTokens +
            bucket.cacheCreateTokens +
            bucket.cacheReadTokens,
          cost: bucket.cost
        })
      }
    } else {
      // 天粒度统计（按日期集合扫描）
      const daysCount = parseInt(days) || 7
      const dayInfos = buildDayInfos(daysCount)

      // 使用索引获取数据，按日期批量查询
      const { usageDataMap, modelDataMap } = await fetchUsageAndModelMaps(dayInfos, (dayInfo) => ({
        usage: [
          `usage:daily:index:${dayInfo.dateStr}`,
          `usage:daily:{id}:${dayInfo.dateStr}`,
          `usage:daily:*:${dayInfo.dateStr}`
        ],
        model: [
          `usage:model:daily:index:${dayInfo.dateStr}`,
          `usage:model:daily:{id}:${dayInfo.dateStr}`,
          `usage:model:daily:*:${dayInfo.dateStr}`
        ]
      }))

      // 按 dateStr 分组
      const modelKeysByDate = groupKeysByRegex(
        modelDataMap.keys(),
        /usage:model:daily:.+?:(\d{4}-\d{2}-\d{2})/
      )
      const usageKeysByDate = groupKeysByRegex(
        usageDataMap.keys(),
        /usage:daily:.+?:(\d{4}-\d{2}-\d{2})/
      )

      // 处理每天的数据
      for (const dayInfo of dayInfos) {
        const bucket = aggregateTrendBucket({
          modelKeys: modelKeysByDate.get(dayInfo.dateStr) || [],
          usageKeys: usageKeysByDate.get(dayInfo.dateStr) || [],
          modelDataMap,
          usageDataMap,
          modelKeyRegex: /usage:model:daily:(.+?):\d{4}-\d{2}-\d{2}/,
          requireUsageKeys: true
        })

        trendData.push({
          date: dayInfo.dateStr,
          inputTokens: bucket.inputTokens,
          outputTokens: bucket.outputTokens,
          requests: bucket.requests,
          cacheCreateTokens: bucket.cacheCreateTokens,
          cacheReadTokens: bucket.cacheReadTokens,
          totalTokens:
            bucket.inputTokens +
            bucket.outputTokens +
            bucket.cacheCreateTokens +
            bucket.cacheReadTokens,
          cost: bucket.cost,
          formattedCost: CostCalculator.formatCost(bucket.cost)
        })
      }
    }

    // 按日期正序排列
    if (granularity === 'hour') {
      trendData.sort((a, b) => new Date(a.hour) - new Date(b.hour))
    } else {
      trendData.sort((a, b) => new Date(a.date) - new Date(b.date))
    }

    return res.json({ success: true, data: trendData, granularity })
  } catch (error) {
    logger.error('❌ Failed to get usage trend:', error)
    return res.status(500).json({ error: 'Failed to get usage trend', message: error.message })
  }
})

// 获取单个API Key的模型统计
router.get('/api-keys/:keyId/model-stats', authenticateAdmin, async (req, res) => {
  try {
    const { keyId } = req.params
    const { period = 'monthly', startDate, endDate } = req.query

    logger.info(
      `📊 Getting model stats for API key: ${keyId}, period: ${period}, startDate: ${startDate}, endDate: ${endDate}`
    )

    const _client = redis.getClientSafe()
    const today = redis.getDateStringInTimezone()
    const tzDate = redis.getDateInTimezone()
    const currentMonth = `${tzDate.getUTCFullYear()}-${String(tzDate.getUTCMonth() + 1).padStart(
      2,
      '0'
    )}`

    let searchPatterns = []

    if (period === 'custom' && startDate && endDate) {
      // 自定义日期范围，生成多个日期的搜索模式
      const start = new Date(startDate)
      const end = new Date(endDate)

      // 确保日期范围有效
      if (start > end) {
        return res.status(400).json({ error: 'Start date must be before or equal to end date' })
      }

      // 限制最大范围为365天
      const daysDiff = Math.ceil((end - start) / (1000 * 60 * 60 * 24)) + 1
      if (daysDiff > 365) {
        return res.status(400).json({ error: 'Date range cannot exceed 365 days' })
      }

      // 生成日期范围内所有日期的搜索模式
      for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
        const dateStr = redis.getDateStringInTimezone(d)
        searchPatterns.push(`usage:${keyId}:model:daily:*:${dateStr}`)
      }

      logger.info(
        `📊 Custom date range patterns: ${searchPatterns.length} days from ${startDate} to ${endDate}`
      )
    } else {
      // 原有的预设期间逻辑
      const pattern =
        period === 'daily'
          ? `usage:${keyId}:model:daily:*:${today}`
          : `usage:${keyId}:model:monthly:*:${currentMonth}`
      searchPatterns = [pattern]
      logger.info(`📊 Preset period pattern: ${pattern}`)
    }

    // 汇总所有匹配的数据
    const modelStatsMap = new Map()
    const modelStats = [] // 定义结果数组

    if (period === 'custom' && startDate && endDate) {
      // 自定义日期范围，使用索引
      const start = new Date(startDate)
      const end = new Date(endDate)
      const fetchPromises = []
      for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
        const dateStr = redis.getDateStringInTimezone(d)
        fetchPromises.push(
          getUsageDataByIndex(
            `usage:keymodel:daily:index:${dateStr}`,
            `usage:{keyId}:model:daily:{model}:${dateStr}`,
            `usage:*:model:daily:*:${dateStr}`
          )
        )
      }
      const allResults = await Promise.all(fetchPromises)
      for (const results of allResults) {
        for (const { key, data } of results) {
          // 过滤出属于该 keyId 的记录
          if (!key.startsWith(`usage:${keyId}:model:`)) {
            continue
          }
          const match = key.match(/usage:.+:model:daily:(.+):\d{4}-\d{2}-\d{2}$/)
          if (!match) {
            continue
          }
          const model = match[1]
          accumulateModelStats(modelStatsMap, model, data)
        }
      }
    } else {
      // 预设期间，使用索引
      let results
      if (period === 'daily') {
        results = await getUsageDataByIndex(
          `usage:keymodel:daily:index:${today}`,
          `usage:{keyId}:model:daily:{model}:${today}`,
          `usage:*:model:daily:*:${today}`
        )
      } else {
        // monthly - 需要月度 keymodel 索引，暂时回退到 SCAN
        const pattern = `usage:${keyId}:model:monthly:*:${currentMonth}`
        results = await redis.scanAndGetAllChunked(pattern)
      }
      for (const { key, data } of results) {
        if (!key.startsWith(`usage:${keyId}:model:`)) {
          continue
        }
        const match =
          key.match(/usage:.+:model:daily:(.+):\d{4}-\d{2}-\d{2}$/) ||
          key.match(/usage:.+:model:monthly:(.+):\d{4}-\d{2}$/)
        if (!match) {
          continue
        }
        const model = match[1]
        accumulateModelStats(modelStatsMap, model, data)
      }
    }

    // 将汇总的数据转换为最终结果
    for (const [model, stats] of modelStatsMap) {
      logger.info(`📊 Model ${model} aggregated data:`, stats)

      let costData
      if (stats.hasStoredCost) {
        // 使用请求时已计算并存储的费用（精确，包含 1M 上下文、Fast Mode 等特殊计费）
        const ratedCost = stats.ratedCostMicro / 1000000
        const realCost = stats.realCostMicro / 1000000
        costData = {
          costs: { total: ratedCost, real: realCost },
          formatted: { total: CostCalculator.formatCost(ratedCost) },
          pricing: null,
          usingDynamicPricing: false,
          usingStoredCost: true
        }
      } else {
        // Legacy fallback：旧数据没有存储费用，从 token 重算
        const usage = {
          input_tokens: stats.inputTokens,
          output_tokens: stats.outputTokens,
          cache_creation_input_tokens: stats.cacheCreateTokens,
          cache_read_input_tokens: stats.cacheReadTokens
        }

        if (stats.ephemeral5mTokens > 0 || stats.ephemeral1hTokens > 0) {
          usage.cache_creation = {
            ephemeral_5m_input_tokens: stats.ephemeral5mTokens,
            ephemeral_1h_input_tokens: stats.ephemeral1hTokens
          }
        }

        costData = CostCalculator.calculateCost(usage, model)
      }

      modelStats.push({
        model,
        requests: stats.requests,
        inputTokens: stats.inputTokens,
        outputTokens: stats.outputTokens,
        cacheCreateTokens: stats.cacheCreateTokens,
        cacheReadTokens: stats.cacheReadTokens,
        allTokens: stats.allTokens,
        // 添加费用信息
        costs: costData.costs,
        formatted: costData.formatted,
        pricing: costData.pricing,
        usingDynamicPricing: costData.usingDynamicPricing
      })
    }

    // 如果没有找到模型级别的详细数据，尝试从汇总数据中生成展示
    if (modelStats.length === 0) {
      logger.info(
        `📊 No detailed model stats found, trying to get aggregate data for API key ${keyId}`
      )

      // 尝试从API Keys列表中获取usage数据作为备选方案
      try {
        const apiKeys = await apiKeyService.getAllApiKeysFast()
        const targetApiKey = apiKeys.find((key) => key.id === keyId)

        if (targetApiKey && targetApiKey.usage) {
          logger.info(
            `📊 Found API key usage data from getAllApiKeys for ${keyId}:`,
            targetApiKey.usage
          )

          // 从汇总数据创建展示条目
          let usageData
          if (period === 'custom' || period === 'daily') {
            // 对于自定义或日统计，使用daily数据或total数据
            usageData = targetApiKey.usage.daily || targetApiKey.usage.total
          } else {
            // 对于月统计，使用monthly数据或total数据
            usageData = targetApiKey.usage.monthly || targetApiKey.usage.total
          }

          if (usageData && usageData.allTokens > 0) {
            const usage = {
              input_tokens: usageData.inputTokens || 0,
              output_tokens: usageData.outputTokens || 0,
              cache_creation_input_tokens: usageData.cacheCreateTokens || 0,
              cache_read_input_tokens: usageData.cacheReadTokens || 0
            }

            // 如果有 ephemeral 5m/1h 拆分数据，添加 cache_creation 子对象以实现精确计费
            const histEph5m = usageData.ephemeral5mTokens || 0
            const histEph1h = usageData.ephemeral1hTokens || 0
            if (histEph5m > 0 || histEph1h > 0) {
              usage.cache_creation = {
                ephemeral_5m_input_tokens: histEph5m,
                ephemeral_1h_input_tokens: histEph1h
              }
            }

            // 对于汇总数据，使用默认模型计算费用
            const costData = CostCalculator.calculateCost(usage, 'claude-3-5-sonnet-20241022')

            modelStats.push({
              model: '总体使用 (历史数据)',
              requests: usageData.requests || 0,
              inputTokens: usageData.inputTokens || 0,
              outputTokens: usageData.outputTokens || 0,
              cacheCreateTokens: usageData.cacheCreateTokens || 0,
              cacheReadTokens: usageData.cacheReadTokens || 0,
              allTokens: usageData.allTokens || 0,
              // 添加费用信息
              costs: costData.costs,
              formatted: costData.formatted,
              pricing: costData.pricing,
              usingDynamicPricing: costData.usingDynamicPricing
            })

            logger.info('📊 Generated display data from API key usage stats')
          } else {
            logger.info(`📊 No usage data found for period ${period} in API key data`)
          }
        } else {
          logger.info(`📊 API key ${keyId} not found or has no usage data`)
        }
      } catch (error) {
        logger.error('❌ Error fetching API key usage data:', error)
      }
    }

    // 按总token数降序排列
    modelStats.sort((a, b) => b.allTokens - a.allTokens)

    logger.info(`📊 Returning ${modelStats.length} model stats for API key ${keyId}:`, modelStats)

    return res.json({ success: true, data: modelStats })
  } catch (error) {
    logger.error('❌ Failed to get API key model stats:', error)
    return res
      .status(500)
      .json({ error: 'Failed to get API key model stats', message: error.message })
  }
})

// 获取按账号分组的使用趋势
router.get('/account-usage-trend', authenticateAdmin, async (req, res) => {
  try {
    const { granularity = 'day', group = 'claude', days = 7, startDate, endDate } = req.query

    const allowedGroups = ['claude', 'openai', 'gemini', 'droid', 'bedrock']
    if (!allowedGroups.includes(group)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid account group'
      })
    }

    const groupLabels = {
      claude: 'Claude账户',
      openai: 'OpenAI账户',
      gemini: 'Gemini账户',
      droid: 'Droid账户',
      bedrock: 'Bedrock账户'
    }

    // 拉取各平台账号列表
    let accounts = []
    if (group === 'claude') {
      const [claudeAccounts, claudeConsoleAccounts] = await Promise.all([
        claudeAccountService.getAllAccounts(),
        claudeConsoleAccountService.getAllAccounts()
      ])

      accounts = [
        ...mapTrendAccounts(
          claudeAccounts,
          'claude',
          (account, shortId) => account.name || account.email || `Claude账号 ${shortId}`
        ),
        ...mapTrendAccounts(
          claudeConsoleAccounts,
          'claude-console',
          (account, shortId) => account.name || `Console账号 ${shortId}`
        )
      ]
    } else if (group === 'openai') {
      const [openaiAccounts, openaiResponsesAccounts] = await Promise.all([
        openaiAccountService.getAllAccounts(),
        openaiResponsesAccountService.getAllAccounts(true)
      ])

      accounts = [
        ...mapTrendAccounts(
          openaiAccounts,
          'openai',
          (account, shortId) => account.name || account.email || `OpenAI账号 ${shortId}`
        ),
        ...mapTrendAccounts(
          openaiResponsesAccounts,
          'openai-responses',
          (account, shortId) => account.name || `Responses账号 ${shortId}`
        )
      ]
    } else if (group === 'gemini') {
      const [geminiAccounts, geminiApiAccounts] = await Promise.all([
        geminiAccountService.getAllAccounts(),
        geminiApiAccountService.getAllAccounts(true)
      ])

      accounts = [
        ...mapTrendAccounts(
          geminiAccounts,
          'gemini',
          (account, shortId) => account.name || account.email || `Gemini账号 ${shortId}`
        ),
        ...mapTrendAccounts(
          geminiApiAccounts,
          'gemini-api',
          (account, shortId) => account.name || `Gemini-API账号 ${shortId}`
        )
      ]
    } else if (group === 'droid') {
      const droidAccounts = await droidAccountService.getAllAccounts()
      accounts = mapTrendAccounts(
        droidAccounts,
        'droid',
        (account, shortId) =>
          account.name || account.ownerEmail || account.ownerName || `Droid账号 ${shortId}`
      )
    } else if (group === 'bedrock') {
      const result = await bedrockAccountService.getAllAccounts()
      const bedrockAccounts = result?.success ? result.data : []
      accounts = mapTrendAccounts(
        bedrockAccounts,
        'bedrock',
        (account, shortId) => account.name || `Bedrock账号 ${shortId}`
      )
    }

    if (!accounts || accounts.length === 0) {
      return res.json({
        success: true,
        data: [],
        granularity,
        group,
        groupLabel: groupLabels[group],
        topAccounts: [],
        totalAccounts: 0
      })
    }

    const accountMap = new Map()
    const accountIdSet = new Set()
    for (const account of accounts) {
      accountMap.set(account.id, {
        name: account.name,
        platform: account.platform
      })
      accountIdSet.add(account.id)
    }

    const fallbackModelByGroup = {
      claude: 'claude-3-5-sonnet-20241022',
      openai: 'gpt-4o-mini-2024-07-18',
      gemini: 'gemini-1.5-flash'
    }
    const fallbackModel = fallbackModelByGroup[group] || 'unknown'

    const trendData = []
    const accountCostTotals = new Map()

    if (granularity === 'hour') {
      const { startTime, endTime } = resolveHourRange(startDate, endDate)
      const hourInfos = buildHourInfos(startTime, endTime)

      // 按小时获取 account_usage 数据（避免全库扫描）
      const { usageDataMap, modelDataMap } = await fetchUsageAndModelMaps(hourInfos, (info) => ({
        usage: [
          `account_usage:hourly:index:${info.hourKey}`,
          `account_usage:hourly:{id}:${info.hourKey}`,
          `account_usage:hourly:*:${info.hourKey}`
        ],
        model: [
          `account_usage:model:hourly:index:${info.hourKey}`,
          `account_usage:model:hourly:{accountId}:{model}:${info.hourKey}`,
          `account_usage:model:hourly:*:${info.hourKey}`
        ]
      }))

      // 按 hourKey 分组
      const usageKeysByHour = groupKeysByRegex(
        usageDataMap.keys(),
        /account_usage:hourly:.+?:(\d{4}-\d{2}-\d{2}:\d{2})/
      )
      const modelKeysByHour = groupKeysByRegex(
        modelDataMap.keys(),
        /account_usage:model:hourly:(.+?):.+?:(\d{4}-\d{2}-\d{2}:\d{2})/,
        (match) => `${match[1]}:${match[2]}`
      )

      // 处理每个小时的数据
      for (const hourInfo of hourInfos) {
        trendData.push({
          hour: hourInfo.isoTime,
          label: hourInfo.label,
          accounts: buildAccountBucketStats({
            usageKeys: usageKeysByHour.get(hourInfo.hourKey) || [],
            usageDataMap,
            modelKeysMap: modelKeysByHour,
            modelDataMap,
            bucketKey: hourInfo.hourKey,
            usageKeyRegex: /account_usage:hourly:(.+?):\d{4}-\d{2}-\d{2}:\d{2}/,
            accountIdSet,
            accountMap,
            fallbackModel,
            accountCostTotals
          })
        })
      }
    } else {
      const daysCount = parseInt(days) || 7
      const dayInfos = buildDayInfos(daysCount)

      // 使用索引获取数据
      const { usageDataMap, modelDataMap } = await fetchUsageAndModelMaps(dayInfos, (info) => ({
        usage: [
          `account_usage:daily:index:${info.dateStr}`,
          `account_usage:daily:{id}:${info.dateStr}`,
          `account_usage:daily:*:${info.dateStr}`
        ],
        model: [
          `account_usage:model:daily:index:${info.dateStr}`,
          `account_usage:model:daily:{accountId}:{model}:${info.dateStr}`,
          `account_usage:model:daily:*:${info.dateStr}`
        ]
      }))

      // 按 dateStr 分组
      const usageKeysByDate = groupKeysByRegex(
        usageDataMap.keys(),
        /account_usage:daily:.+?:(\d{4}-\d{2}-\d{2})/
      )
      const modelKeysByDate = groupKeysByRegex(
        modelDataMap.keys(),
        /account_usage:model:daily:(.+?):.+?:(\d{4}-\d{2}-\d{2})/,
        (match) => `${match[1]}:${match[2]}`
      )

      // 处理每天的数据
      for (const dayInfo of dayInfos) {
        trendData.push({
          date: dayInfo.dateStr,
          accounts: buildAccountBucketStats({
            usageKeys: usageKeysByDate.get(dayInfo.dateStr) || [],
            usageDataMap,
            modelKeysMap: modelKeysByDate,
            modelDataMap,
            bucketKey: dayInfo.dateStr,
            usageKeyRegex: /account_usage:daily:(.+?):\d{4}-\d{2}-\d{2}/,
            accountIdSet,
            accountMap,
            fallbackModel,
            accountCostTotals
          })
        })
      }
    }

    if (granularity === 'hour') {
      trendData.sort((a, b) => new Date(a.hour) - new Date(b.hour))
    } else {
      trendData.sort((a, b) => new Date(a.date) - new Date(b.date))
    }

    const topAccounts = Array.from(accountCostTotals.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 20)
      .map(([accountId]) => accountId)

    return res.json({
      success: true,
      data: trendData,
      granularity,
      group,
      groupLabel: groupLabels[group],
      topAccounts,
      totalAccounts: accountCostTotals.size
    })
  } catch (error) {
    logger.error('❌ Failed to get account usage trend:', error)
    return res
      .status(500)
      .json({ error: 'Failed to get account usage trend', message: error.message })
  }
})

// 获取按API Key分组的使用趋势
router.get('/api-keys-usage-trend', authenticateAdmin, async (req, res) => {
  try {
    const { granularity = 'day', days = 7, startDate, endDate } = req.query

    logger.info(`📊 Getting API keys usage trend, granularity: ${granularity}, days: ${days}`)

    const trendData = []

    // 获取所有API Keys（只需要 id 和 name，过滤已删除的）
    const apiKeyIds = await redis.scanApiKeyIds()
    const apiKeyBasicData = await redis.batchGetApiKeys(apiKeyIds)
    const apiKeyMap = new Map(
      apiKeyBasicData.filter((key) => !key.isDeleted).map((key) => [key.id, key])
    )

    if (granularity === 'hour') {
      // 小时粒度统计
      const { startTime, endTime } = resolveHourRange(startDate, endDate)
      const hourInfos = buildHourInfos(startTime, endTime)

      // 使用索引获取数据，按小时批量查询
      const { usageDataMap, modelDataMap } = await fetchUsageAndModelMaps(hourInfos, (info) => ({
        usage: [
          `usage:hourly:index:${info.hourKey}`,
          `usage:hourly:{id}:${info.hourKey}`,
          `usage:hourly:*:${info.hourKey}`
        ],
        model: [
          `usage:keymodel:hourly:index:${info.hourKey}`,
          `usage:{keyId}:model:hourly:{model}:${info.hourKey}`,
          `usage:*:model:hourly:*:${info.hourKey}`
        ]
      }))

      // 按 hourKey 分组 keys
      const usageKeysByHour = groupKeysByRegex(
        usageDataMap.keys(),
        /usage:hourly:.+?:(\d{4}-\d{2}-\d{2}:\d{2})/
      )
      const modelKeysByHour = groupKeysByRegex(
        modelDataMap.keys(),
        /usage:.+?:model:hourly:.+?:(\d{4}-\d{2}-\d{2}:\d{2})/
      )

      // 处理每个小时的数据
      for (const hourInfo of hourInfos) {
        trendData.push({
          hour: hourInfo.isoTime,
          label: hourInfo.label,
          apiKeys: buildApiKeyBucketStats({
            usageKeys: usageKeysByHour.get(hourInfo.hourKey) || [],
            modelKeys: modelKeysByHour.get(hourInfo.hourKey) || [],
            usageDataMap,
            modelDataMap,
            usageKeyRegex: /usage:hourly:(.+?):\d{4}-\d{2}-\d{2}:\d{2}/,
            modelKeyRegex: /usage:(.+?):model:hourly:(.+?):\d{4}-\d{2}-\d{2}:\d{2}/,
            apiKeyMap
          })
        })
      }
    } else {
      // 天粒度统计（按日期集合扫描）
      const daysCount = parseInt(days) || 7
      const dayInfos = buildDayInfos(daysCount)

      // 使用索引获取数据，按日期批量查询
      const { usageDataMap, modelDataMap } = await fetchUsageAndModelMaps(dayInfos, (info) => ({
        usage: [
          `usage:daily:index:${info.dateStr}`,
          `usage:daily:{id}:${info.dateStr}`,
          `usage:daily:*:${info.dateStr}`
        ],
        model: [
          `usage:keymodel:daily:index:${info.dateStr}`,
          `usage:{keyId}:model:daily:{model}:${info.dateStr}`,
          `usage:*:model:daily:*:${info.dateStr}`
        ]
      }))

      // 按 dateStr 分组 keys
      const usageKeysByDate = groupKeysByRegex(
        usageDataMap.keys(),
        /usage:daily:.+?:(\d{4}-\d{2}-\d{2})/
      )
      const modelKeysByDate = groupKeysByRegex(
        modelDataMap.keys(),
        /usage:.+?:model:daily:.+?:(\d{4}-\d{2}-\d{2})/
      )

      // 处理每天的数据
      for (const dayInfo of dayInfos) {
        trendData.push({
          date: dayInfo.dateStr,
          apiKeys: buildApiKeyBucketStats({
            usageKeys: usageKeysByDate.get(dayInfo.dateStr) || [],
            modelKeys: modelKeysByDate.get(dayInfo.dateStr) || [],
            usageDataMap,
            modelDataMap,
            usageKeyRegex: /usage:daily:(.+?):\d{4}-\d{2}-\d{2}/,
            modelKeyRegex: /usage:(.+?):model:daily:(.+?):\d{4}-\d{2}-\d{2}/,
            apiKeyMap
          })
        })
      }
    }

    // 按时间正序排列
    if (granularity === 'hour') {
      trendData.sort((a, b) => new Date(a.hour) - new Date(b.hour))
    } else {
      trendData.sort((a, b) => new Date(a.date) - new Date(b.date))
    }

    // 计算每个API Key的总token数，用于排序
    const apiKeyTotals = new Map()
    for (const point of trendData) {
      for (const [apiKeyId, data] of Object.entries(point.apiKeys)) {
        apiKeyTotals.set(apiKeyId, (apiKeyTotals.get(apiKeyId) || 0) + data.tokens)
      }
    }

    // 获取前10个使用量最多的API Key
    const topApiKeys = Array.from(apiKeyTotals.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([apiKeyId]) => apiKeyId)

    return res.json({
      success: true,
      data: trendData,
      granularity,
      topApiKeys,
      totalApiKeys: apiKeyTotals.size
    })
  } catch (error) {
    logger.error('❌ Failed to get API keys usage trend:', error)
    return res
      .status(500)
      .json({ error: 'Failed to get API keys usage trend', message: error.message })
  }
})

// 计算总体使用费用
router.get('/usage-costs', authenticateAdmin, async (req, res) => {
  try {
    const { period = 'all' } = req.query // all, today, monthly, 7days

    logger.info(`💰 Calculating usage costs for period: ${period}`)

    // 模型名标准化函数（与redis.js保持一致）
    const normalizeModelName = (model) => {
      if (!model || model === 'unknown') {
        return model
      }

      // 对于Bedrock模型，去掉区域前缀进行统一
      if (model.includes('.anthropic.') || model.includes('.claude')) {
        // 匹配所有AWS区域格式：region.anthropic.model-name-v1:0 -> claude-model-name
        // 支持所有AWS区域格式，如：us-east-1, eu-west-1, ap-southeast-1, ca-central-1等
        let normalized = model.replace(/^[a-z0-9-]+\./, '') // 去掉任何区域前缀（更通用）
        normalized = normalized.replace('anthropic.', '') // 去掉anthropic前缀
        normalized = normalized.replace(/-v\d+:\d+$/, '') // 去掉版本后缀（如-v1:0, -v2:1等）
        return normalized
      }

      // 对于其他模型，去掉常见的版本后缀
      return model.replace(/-v\d+:\d+$|:latest$/, '')
    }

    const totalCosts = {
      inputCost: 0,
      outputCost: 0,
      cacheCreateCost: 0,
      cacheReadCost: 0,
      totalCost: 0
    }

    const modelCosts = {}

    // 按模型统计费用
    const _client = redis.getClientSafe()
    const today = redis.getDateStringInTimezone()
    const tzDate = redis.getDateInTimezone()
    const currentMonth = `${tzDate.getUTCFullYear()}-${String(tzDate.getUTCMonth() + 1).padStart(
      2,
      '0'
    )}`

    let _pattern
    if (period === 'today') {
      _pattern = `usage:model:daily:*:${today}`
    } else if (period === 'monthly') {
      _pattern = `usage:model:monthly:*:${currentMonth}`
    } else if (period === '7days') {
      // 最近7天：汇总daily数据（使用 SCAN + Pipeline 优化）
      const modelUsageMap = new Map()

      // 收集最近7天的所有日期
      const dateStrs = []
      for (let i = 0; i < 7; i++) {
        const date = new Date()
        date.setDate(date.getDate() - i)
        const currentTzDate = redis.getDateInTimezone(date)
        const dateStr = `${currentTzDate.getUTCFullYear()}-${String(
          currentTzDate.getUTCMonth() + 1
        ).padStart(2, '0')}-${String(currentTzDate.getUTCDate()).padStart(2, '0')}`
        dateStrs.push(dateStr)
      }

      // 使用索引获取数据
      const fetchPromises = dateStrs.map((dateStr) =>
        getUsageDataByIndex(
          `usage:model:daily:index:${dateStr}`,
          `usage:model:daily:{id}:${dateStr}`,
          `usage:model:daily:*:${dateStr}`
        )
      )
      const allResults = await Promise.all(fetchPromises)
      const allData = allResults.flat()

      // 处理数据
      for (const { key, data } of allData) {
        if (!data) {
          continue
        }

        const modelMatch = key.match(/usage:model:daily:(.+):\d{4}-\d{2}-\d{2}$/)
        if (!modelMatch) {
          continue
        }

        const rawModel = modelMatch[1]
        const normalizedModel = normalizeModelName(rawModel)
        accumulateModelUsage(modelUsageMap, normalizedModel, data)
      }

      // 计算7天统计的费用
      logger.info(`💰 Processing ${modelUsageMap.size} unique models for 7days cost calculation`)
      summarizeModelUsageCosts(modelUsageMap, totalCosts, modelCosts, ' (7days)')

      // 返回7天统计结果
      return res.json({
        success: true,
        data: {
          period,
          totalCosts: formatTotalCosts(totalCosts),
          modelCosts: Object.values(modelCosts)
        }
      })
    } else {
      // 全部时间，使用月份索引
      const months = await redis.client.smembers('usage:model:monthly:months')
      const allData = []
      if (months && months.length > 0) {
        const fetchPromises = months.map((month) =>
          getUsageDataByIndex(
            `usage:model:monthly:index:${month}`,
            `usage:model:monthly:{id}:${month}`,
            `usage:model:monthly:*:${month}`
          )
        )
        const results = await Promise.all(fetchPromises)
        results.forEach((r) => allData.push(...r))
      }
      logger.info(`💰 Total period calculation: found ${allData.length} monthly model keys`)

      if (allData.length > 0) {
        const modelUsageMap = new Map()

        for (const { key, data } of allData) {
          if (!data) {
            continue
          }

          const modelMatch = key.match(/usage:model:monthly:(.+):(\d{4}-\d{2})$/)
          if (!modelMatch) {
            continue
          }

          const model = modelMatch[1]
          accumulateModelUsage(modelUsageMap, model, data)
        }

        // 使用模型级别的数据计算费用
        logger.info(`💰 Processing ${modelUsageMap.size} unique models for total cost calculation`)
        summarizeModelUsageCosts(modelUsageMap, totalCosts, modelCosts)
      } else {
        // 如果没有详细的模型统计数据，回退到API Key汇总数据（延迟加载）
        logger.warn('No detailed model statistics found, falling back to API Key aggregated data')
        const apiKeys = await apiKeyService.getAllApiKeysFast()

        for (const apiKey of apiKeys) {
          if (apiKey.usage && apiKey.usage.total) {
            // 使用加权平均价格计算（基于当前活跃模型的价格分布）
            const costResult = CostCalculator.calculateCost(
              buildBillingUsage(apiKey.usage.total),
              'claude-3-5-haiku-20241022'
            )
            addCostResult(totalCosts, costResult)
          }
        }
      }

      return res.json({
        success: true,
        data: {
          period,
          totalCosts: formatTotalCosts(totalCosts),
          modelCosts: Object.values(modelCosts).sort((a, b) => b.costs.total - a.costs.total),
          pricingServiceStatus: pricingService.getStatus()
        }
      })
    }

    // 对于今日或本月，使用索引查询
    let allData
    if (period === 'today') {
      const results = await getUsageDataByIndex(
        `usage:model:daily:index:${today}`,
        `usage:model:daily:{id}:${today}`,
        `usage:model:daily:*:${today}`
      )
      allData = results
    } else {
      // 本月 - 使用月度索引
      const results = await getUsageDataByIndex(
        `usage:model:monthly:index:${currentMonth}`,
        `usage:model:monthly:{id}:${currentMonth}`,
        `usage:model:monthly:*:${currentMonth}`
      )
      allData = results
    }
    const regex =
      period === 'today'
        ? /usage:model:daily:(.+):\d{4}-\d{2}-\d{2}$/
        : /usage:model:monthly:(.+):\d{4}-\d{2}$/

    for (const { key, data } of allData) {
      if (!data) {
        continue
      }

      const match = key.match(regex)
      if (!match) {
        continue
      }

      const model = match[1]
      const usage = buildBillingUsage(data)
      const costResult = CostCalculator.calculateCost(usage, model)

      // 累加总费用
      addCostResult(totalCosts, costResult)

      // 记录模型费用
      modelCosts[model] = {
        model,
        requests: parseInt(data.requests) || 0,
        usage,
        costs: costResult.costs,
        formatted: costResult.formatted,
        usingDynamicPricing: costResult.usingDynamicPricing
      }
    }

    return res.json({
      success: true,
      data: {
        period,
        totalCosts: formatTotalCosts(totalCosts),
        modelCosts: Object.values(modelCosts).sort((a, b) => b.costs.total - a.costs.total),
        pricingServiceStatus: pricingService.getStatus()
      }
    })
  } catch (error) {
    logger.error('❌ Failed to calculate usage costs:', error)
    return res
      .status(500)
      .json({ error: 'Failed to calculate usage costs', message: error.message })
  }
})

// 获取 API Key 的请求记��时间线
router.get('/api-keys/:keyId/usage-records', authenticateAdmin, async (req, res) => {
  try {
    const { keyId } = req.params
    const { model, accountId } = req.query

    const { pageNumber, pageSizeNumber, normalizedSortOrder, startTime, endTime, invalidRange } =
      parseRecordQuery(req.query)

    if (invalidRange) {
      return res.status(400).json({ success: false, error: 'Invalid date range' })
    }

    if (startTime && endTime && startTime > endTime) {
      return res
        .status(400)
        .json({ success: false, error: 'Start date must be before or equal to end date' })
    }

    const apiKeyInfo = await redis.getApiKey(keyId)
    if (!apiKeyInfo || Object.keys(apiKeyInfo).length === 0) {
      return res.status(404).json({ success: false, error: 'API key not found' })
    }

    const rawRecords = await redis.getUsageRecords(keyId, 5000)

    const accountServices = [
      { type: 'claude', getter: (id) => claudeAccountService.getAccount(id) },
      { type: 'claude-console', getter: (id) => claudeConsoleAccountService.getAccount(id) },
      { type: 'ccr', getter: (id) => ccrAccountService.getAccount(id) },
      { type: 'openai', getter: (id) => openaiAccountService.getAccount(id) },
      { type: 'openai-responses', getter: (id) => openaiResponsesAccountService.getAccount(id) },
      { type: 'gemini', getter: (id) => geminiAccountService.getAccount(id) },
      { type: 'gemini-api', getter: (id) => geminiApiAccountService.getAccount(id) },
      { type: 'droid', getter: (id) => droidAccountService.getAccount(id) }
    ]

    const accountCache = new Map()
    const resolveAccountInfo = async (id, type) => {
      if (!id) {
        return null
      }

      const cacheKey = `${type || 'any'}:${id}`
      if (accountCache.has(cacheKey)) {
        return accountCache.get(cacheKey)
      }

      let servicesToTry = type
        ? accountServices.filter((svc) => svc.type === type)
        : accountServices

      // 若渠道改名或传入未知类型，回退尝试全量服务，避免漏解析历史账号
      if (!servicesToTry.length) {
        servicesToTry = accountServices
      }

      for (const service of servicesToTry) {
        try {
          const account = await service.getter(id)
          if (account) {
            const info = {
              id,
              name: account.name || account.email || id,
              type: service.type,
              status: account.status || account.isActive
            }
            accountCache.set(cacheKey, info)
            return info
          }
        } catch (error) {
          logger.debug(`⚠️ Failed to resolve account ${id} via ${service.type}: ${error.message}`)
        }
      }

      accountCache.set(cacheKey, null)
      return null
    }

    const filteredRecords = rawRecords.filter((record) => {
      if (!isRecordWithinRange(record, startTime, endTime)) {
        return false
      }
      if (model && record.model !== model) {
        return false
      }
      if (accountId && record.accountId !== accountId) {
        return false
      }
      return true
    })

    sortRecordsByTimestamp(filteredRecords, normalizedSortOrder)

    const summary = {
      totalRequests: 0,
      inputTokens: 0,
      outputTokens: 0,
      cacheCreateTokens: 0,
      cacheReadTokens: 0,
      totalTokens: 0,
      totalCost: 0
    }

    const modelSet = new Set()
    const accountOptionMap = new Map()
    let earliestTimestamp = null
    let latestTimestamp = null

    for (const record of filteredRecords) {
      const { usage, totalTokens, computedCost } = computeRecordCost(record)
      addRecordToSummary(summary, usage, totalTokens, computedCost)

      if (record.model) {
        modelSet.add(record.model)
      }

      if (record.accountId) {
        const normalizedType = record.accountType || 'unknown'
        if (!accountOptionMap.has(record.accountId)) {
          accountOptionMap.set(record.accountId, {
            id: record.accountId,
            accountTypes: new Set([normalizedType])
          })
        } else {
          accountOptionMap.get(record.accountId).accountTypes.add(normalizedType)
        }
      }

      if (record.timestamp) {
        const ts = new Date(record.timestamp)
        if (!Number.isNaN(ts.getTime())) {
          if (!earliestTimestamp || ts < earliestTimestamp) {
            earliestTimestamp = ts
          }
          if (!latestTimestamp || ts > latestTimestamp) {
            latestTimestamp = ts
          }
        }
      }
    }

    const { pageRecords, pagination } = paginateRecords(filteredRecords, pageNumber, pageSizeNumber)

    const enrichedRecords = []
    for (const record of pageRecords) {
      const { usage, costData, computedCost, realCost, totalTokens } = computeRecordCost(record)

      const accountInfo = await resolveAccountInfo(record.accountId, record.accountType)
      const resolvedAccountType = accountInfo?.type || record.accountType || 'unknown'

      enrichedRecords.push({
        timestamp: record.timestamp,
        model: record.model || 'unknown',
        actualModel: record.actualModel || record.model || 'unknown',
        requestedModel: record.requestedModel || null,
        displayModel: record.displayModel || record.model || 'unknown',
        accountId: record.accountId || null,
        accountName: accountInfo?.name || null,
        accountStatus: accountInfo?.status ?? null,
        accountType: resolvedAccountType,
        accountTypeName: accountTypeNames[resolvedAccountType] || '未知渠道',
        inputTokens: usage.input_tokens,
        outputTokens: usage.output_tokens,
        cacheCreateTokens: usage.cache_creation_input_tokens,
        cacheReadTokens: usage.cache_read_input_tokens,
        ephemeral5mTokens: record.ephemeral5mTokens || 0,
        ephemeral1hTokens: record.ephemeral1hTokens || 0,
        totalTokens,
        isLongContextRequest: record.isLongContext || record.isLongContextRequest || false,
        cost: Number(computedCost.toFixed(6)),
        costFormatted: CostCalculator.formatCost(computedCost),
        realCost: Number(realCost.toFixed(6)),
        realCostFormatted: CostCalculator.formatCost(realCost),
        costBreakdown: buildCostBreakdown(record, costData, computedCost),
        responseTime: record.responseTime || null
      })
    }

    const accountOptions = []
    for (const option of accountOptionMap.values()) {
      const types = Array.from(option.accountTypes || [])

      // 优先按历史出现的 accountType 解析，若失败则回退全量解析
      let resolvedInfo = null
      for (const type of types) {
        resolvedInfo = await resolveAccountInfo(option.id, type)
        if (resolvedInfo && resolvedInfo.name) {
          break
        }
      }
      if (!resolvedInfo) {
        resolvedInfo = await resolveAccountInfo(option.id)
      }

      const chosenType = resolvedInfo?.type || types[0] || 'unknown'
      const chosenTypeName = accountTypeNames[chosenType] || '未知渠道'

      if (!resolvedInfo) {
        logger.warn(`⚠️ 保留无法解析的账户筛选项: ${option.id}, types=${types.join(',') || 'none'}`)
      }

      accountOptions.push({
        id: option.id,
        name: resolvedInfo?.name || option.id,
        accountType: chosenType,
        accountTypeName: chosenTypeName,
        rawTypes: types
      })
    }

    return res.json({
      success: true,
      data: {
        records: enrichedRecords,
        pagination,
        filters: {
          startDate: startTime ? startTime.toISOString() : null,
          endDate: endTime ? endTime.toISOString() : null,
          model: model || null,
          accountId: accountId || null,
          sortOrder: normalizedSortOrder
        },
        apiKeyInfo: {
          id: keyId,
          name: apiKeyInfo.name || apiKeyInfo.label || keyId
        },
        summary: finalizeRecordSummary(summary),
        availableFilters: {
          models: Array.from(modelSet),
          accounts: accountOptions,
          dateRange: {
            earliest: earliestTimestamp ? earliestTimestamp.toISOString() : null,
            latest: latestTimestamp ? latestTimestamp.toISOString() : null
          }
        }
      }
    })
  } catch (error) {
    logger.error('❌ Failed to get API key usage records:', error)
    return res
      .status(500)
      .json({ error: 'Failed to get API key usage records', message: error.message })
  }
})

// 获取账户的请求记录时间线
router.get('/accounts/:accountId/usage-records', authenticateAdmin, async (req, res) => {
  try {
    const { accountId } = req.params
    const { platform, model, apiKeyId } = req.query

    const { pageNumber, pageSizeNumber, normalizedSortOrder, startTime, endTime, invalidRange } =
      parseRecordQuery(req.query)

    if (invalidRange) {
      return res.status(400).json({ success: false, error: 'Invalid date range' })
    }

    if (startTime && endTime && startTime > endTime) {
      return res
        .status(400)
        .json({ success: false, error: 'Start date must be before or equal to end date' })
    }

    const accountInfo = await resolveAccountByPlatform(accountId, platform)
    if (!accountInfo) {
      return res.status(404).json({ success: false, error: 'Account not found' })
    }

    const allApiKeys = await apiKeyService.getAllApiKeysFast(true)
    const apiKeyNameCache = new Map(
      allApiKeys.map((key) => [key.id, key.name || key.label || key.id])
    )

    let keysToUse = apiKeyId ? allApiKeys.filter((key) => key.id === apiKeyId) : allApiKeys
    if (apiKeyId && keysToUse.length === 0) {
      keysToUse = [{ id: apiKeyId }]
    }

    const missingUsageStatuses = new Set([
      'started',
      'aborted',
      'stream_error',
      'completed_without_usage',
      'record_failed'
    ])
    const usageStatusNames = {
      started: '已发起',
      completed: '已完成',
      aborted: '客户端中断',
      stream_error: '流错误',
      completed_without_usage: '无用量结束',
      record_failed: '记录失败'
    }
    const getUsageStatus = (record) => record.usageStatus || record.lifecycleStatus || 'completed'
    const isUsageMissingRecord = (record) =>
      record.usageMissing === true || missingUsageStatuses.has(getUsageStatus(record))

    const filteredRecords = []
    const modelSet = new Set()
    const apiKeyOptionMap = new Map()
    let earliestTimestamp = null
    let latestTimestamp = null

    const batchSize = 10
    for (let i = 0; i < keysToUse.length; i += batchSize) {
      const batch = keysToUse.slice(i, i + batchSize)
      const batchResults = await Promise.all(
        batch.map(async (key) => {
          try {
            const records = await redis.getUsageRecords(key.id, 5000)
            return { keyId: key.id, records: records || [] }
          } catch (error) {
            logger.debug(`⚠️ Failed to get usage records for key ${key.id}: ${error.message}`)
            return { keyId: key.id, records: [] }
          }
        })
      )

      for (const { keyId, records } of batchResults) {
        const apiKeyName = apiKeyNameCache.get(keyId) || (await getApiKeyName(keyId))
        for (const record of records) {
          if (record.accountId !== accountId) {
            continue
          }
          if (!isRecordWithinRange(record, startTime, endTime)) {
            continue
          }
          if (model && record.model !== model) {
            continue
          }

          const accountType = record.accountType || accountInfo.platform || 'unknown'
          const normalizedModel = record.model || 'unknown'

          modelSet.add(normalizedModel)
          apiKeyOptionMap.set(keyId, { id: keyId, name: apiKeyName })

          if (record.timestamp) {
            const ts = new Date(record.timestamp)
            if (!Number.isNaN(ts.getTime())) {
              if (!earliestTimestamp || ts < earliestTimestamp) {
                earliestTimestamp = ts
              }
              if (!latestTimestamp || ts > latestTimestamp) {
                latestTimestamp = ts
              }
            }
          }

          filteredRecords.push({
            ...record,
            model: normalizedModel,
            accountType,
            apiKeyId: keyId,
            apiKeyName
          })
        }
      }
    }

    sortRecordsByTimestamp(filteredRecords, normalizedSortOrder)

    const summary = {
      totalRequests: 0,
      inputTokens: 0,
      outputTokens: 0,
      cacheCreateTokens: 0,
      cacheReadTokens: 0,
      totalTokens: 0,
      totalCost: 0,
      completedRequests: 0,
      missingUsageRequests: 0,
      abortedRequests: 0,
      streamErrorRequests: 0
    }

    for (const record of filteredRecords) {
      const { usage, totalTokens, computedCost } = computeRecordCost(record)
      addRecordToSummary(summary, usage, totalTokens, computedCost)

      const usageStatus = getUsageStatus(record)
      if (usageStatus === 'completed') {
        summary.completedRequests += 1
      }
      if (isUsageMissingRecord(record)) {
        summary.missingUsageRequests += 1
      }
      if (usageStatus === 'aborted') {
        summary.abortedRequests += 1
      }
      if (usageStatus === 'stream_error') {
        summary.streamErrorRequests += 1
      }
    }

    const { pageRecords, pagination } = paginateRecords(filteredRecords, pageNumber, pageSizeNumber)

    const enrichedRecords = []
    for (const record of pageRecords) {
      const { usage, costData, computedCost, realCost, totalTokens } = computeRecordCost(record)
      const usageStatus = getUsageStatus(record)
      const usageMissing = isUsageMissingRecord(record)

      enrichedRecords.push({
        timestamp: record.timestamp,
        model: record.model || 'unknown',
        actualModel: record.actualModel || record.model || 'unknown',
        requestedModel: record.requestedModel || null,
        displayModel: record.displayModel || record.model || 'unknown',
        apiKeyId: record.apiKeyId,
        apiKeyName: record.apiKeyName,
        accountId,
        accountName: accountInfo.name || accountInfo.email || accountId,
        accountType: record.accountType,
        accountTypeName: accountTypeNames[record.accountType] || '未知渠道',
        usageStatus,
        usageStatusName: usageStatusNames[usageStatus] || usageStatus,
        usageMissing,
        billableUsageUnknown: record.billableUsageUnknown === true || usageMissing,
        statusMessage: record.statusMessage || null,
        failureReason: record.failureReason || null,
        lifecycleRecordId: record.lifecycleRecordId || record.requestLifecycleId || null,
        startedAt: record.startedAt || null,
        endedAt: record.endedAt || record.completedAt || null,
        inputTokens: usage.input_tokens,
        outputTokens: usage.output_tokens,
        cacheCreateTokens: usage.cache_creation_input_tokens,
        cacheReadTokens: usage.cache_read_input_tokens,
        ephemeral5mTokens: record.ephemeral5mTokens || 0,
        ephemeral1hTokens: record.ephemeral1hTokens || 0,
        totalTokens,
        isLongContextRequest: record.isLongContext || record.isLongContextRequest || false,
        cost: Number(computedCost.toFixed(6)),
        costFormatted: CostCalculator.formatCost(computedCost),
        realCost: Number(realCost.toFixed(6)),
        realCostFormatted: CostCalculator.formatCost(realCost),
        costBreakdown: buildCostBreakdown(record, costData, computedCost),
        responseTime: record.responseTime || null
      })
    }

    return res.json({
      success: true,
      data: {
        records: enrichedRecords,
        pagination,
        filters: {
          startDate: startTime ? startTime.toISOString() : null,
          endDate: endTime ? endTime.toISOString() : null,
          model: model || null,
          apiKeyId: apiKeyId || null,
          platform: accountInfo.platform,
          sortOrder: normalizedSortOrder
        },
        accountInfo: {
          id: accountId,
          name: accountInfo.name || accountInfo.email || accountId,
          platform: accountInfo.platform || platform || 'unknown',
          status: accountInfo.status ?? accountInfo.isActive ?? null
        },
        summary: finalizeRecordSummary(summary),
        availableFilters: {
          models: Array.from(modelSet),
          apiKeys: Array.from(apiKeyOptionMap.values()),
          dateRange: {
            earliest: earliestTimestamp ? earliestTimestamp.toISOString() : null,
            latest: latestTimestamp ? latestTimestamp.toISOString() : null
          }
        }
      }
    })
  } catch (error) {
    logger.error('❌ Failed to get account usage records:', error)
    return res
      .status(500)
      .json({ error: 'Failed to get account usage records', message: error.message })
  }
})

module.exports = router
