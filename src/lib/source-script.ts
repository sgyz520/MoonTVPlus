/* eslint-disable @typescript-eslint/no-explicit-any */

import * as cheerio from 'cheerio';
import { nanoid } from 'nanoid';

import { db } from '@/lib/db';

const SOURCE_SCRIPT_REGISTRY_KEY = 'source-script:registry';
const MAX_HISTORY_ITEMS = 10;
const DEFAULT_TIMEOUT_MS = 20000;

export interface SourceScriptVersion {
  version: string;
  code: string;
  updatedAt: number;
}

export interface SourceScriptRecord {
  id: string;
  key: string;
  name: string;
  description?: string;
  enabled: boolean;
  version: string;
  code: string;
  createdAt: number;
  updatedAt: number;
  history: SourceScriptVersion[];
}

export interface SourceScriptImportItem {
  key: string;
  name: string;
  description?: string;
  code: string;
  enabled?: boolean;
}

export interface SourceScriptRegistry {
  items: SourceScriptRecord[];
}

export interface SourceScriptTestResult {
  ok: boolean;
  durationMs: number;
  logs: string[];
  meta?: Record<string, any>;
  result?: any;
  error?: string;
}

export type SourceScriptHook =
  | 'getSources'
  | 'search'
  | 'recommend'
  | 'detail'
  | 'resolvePlayUrl';

export interface PublicSourceScriptSummary {
  id: string;
  key: string;
  name: string;
  description?: string;
  version: string;
  updatedAt: number;
}

export interface ScriptSourceDescriptor {
  id: string;
  name: string;
}

const SCRIPT_SOURCE_PREFIX = 'script:';
const SCRIPT_EPISODE_PREFIX = '__script_ep__';

const DEFAULT_SCRIPT_TEMPLATE = `return {
  meta: {
    name: '示例脚本',
    author: 'admin'
  },

  async getSources(ctx) {
    return [
      { id: 'main', name: '主站' },
      { id: 'backup', name: '备用站' }
    ];
  },

  async search(ctx, { keyword, page, sourceId }) {
    ctx.log.info('search', keyword, page, sourceId);
    return {
      sourceId,
      list: [],
      page,
      pageCount: 1,
      total: 0
    };
  },

  async recommend(ctx, { page }) {
    ctx.log.info('recommend', page);
    return {
      list: [],
      page: page || 1,
      pageCount: 1,
      total: 0
    };
  },

  async detail(ctx, { id, sourceId }) {
    ctx.log.info('detail', id, sourceId);
    return {
      id,
      sourceId,
      title: '',
      poster: '',
      year: '',
      desc: '',
      playbacks: [
        {
          sourceId: sourceId || 'main',
          sourceName: '主站',
          lineId: 'default',
          lineName: '默认线路',
          episodes: [],
          episodes_titles: []
        }
      ]
    };
  },

  async resolvePlayUrl(ctx, { playUrl, sourceId, lineId, episodeIndex }) {
    ctx.log.info('resolvePlayUrl', sourceId, lineId, episodeIndex, playUrl);
    return {
      url: playUrl,
      type: 'auto',
      headers: {}
    };
  }
};`;

function getNowVersion() {
  return new Date().toISOString();
}

function buildEmptyRegistry(): SourceScriptRegistry {
  return { items: [] };
}

async function loadRegistry(): Promise<SourceScriptRegistry> {
  const raw = await db.getGlobalValue(SOURCE_SCRIPT_REGISTRY_KEY);
  if (!raw) {
    return buildEmptyRegistry();
  }

  try {
    const parsed = JSON.parse(raw) as SourceScriptRegistry;
    if (!parsed || !Array.isArray(parsed.items)) {
      return buildEmptyRegistry();
    }
    return parsed;
  } catch {
    return buildEmptyRegistry();
  }
}

async function saveRegistry(registry: SourceScriptRegistry) {
  await db.setGlobalValue(
    SOURCE_SCRIPT_REGISTRY_KEY,
    JSON.stringify(registry)
  );
}

function assertScriptKey(key: string) {
  if (!/^[a-zA-Z0-9_-]+$/.test(key)) {
    throw new Error('脚本 Key 仅支持字母、数字、下划线和中划线');
  }
}

function assertSafeCode(code: string) {
  const blockedPatterns = [
    /\brequire\s*\(/,
    /\bprocess\./,
    /\bchild_process\b/,
    /\bfs\b/,
    /\bimport\s*\(/,
    /\beval\s*\(/,
    /\bnew\s+Function\b/,
  ];

  for (const pattern of blockedPatterns) {
    if (pattern.test(code)) {
      throw new Error(`脚本包含不允许的用法: ${pattern}`);
    }
  }
}

function createLogCollector() {
  const logs: string[] = [];

  const push = (level: string, args: any[]) => {
    const rendered = args
      .map((arg) => {
        if (typeof arg === 'string') return arg;
        try {
          return JSON.stringify(arg);
        } catch {
          return String(arg);
        }
      })
      .join(' ');

    logs.push(`[${level}] ${rendered}`);
    if (logs.length > 50) {
      logs.shift();
    }
  };

  return {
    logs,
    log: {
      info: (...args: any[]) => push('info', args),
      warn: (...args: any[]) => push('warn', args),
      error: (...args: any[]) => push('error', args),
    },
  };
}

function withTimeout<T>(promise: Promise<T>, timeoutMs = DEFAULT_TIMEOUT_MS) {
  return Promise.race([
    promise,
    new Promise<T>((_, reject) => {
      setTimeout(() => reject(new Error(`执行超时(${timeoutMs}ms)`)), timeoutMs);
    }),
  ]);
}

function createCacheHelpers(scriptId: string) {
  const prefix = `source-script-cache:${scriptId}:`;

  return {
    async get(key: string) {
      const raw = await db.getGlobalValue(`${prefix}${key}`);
      if (!raw) {
        return null;
      }

      try {
        const parsed = JSON.parse(raw) as { value: string; expiresAt: number };
        if (parsed.expiresAt && parsed.expiresAt < Date.now()) {
          await db.deleteGlobalValue(`${prefix}${key}`);
          return null;
        }
        return parsed.value ?? null;
      } catch {
        return raw;
      }
    },
    async set(key: string, value: string, ttlSec = 300) {
      await db.setGlobalValue(
        `${prefix}${key}`,
        JSON.stringify({
          value,
          expiresAt: Date.now() + ttlSec * 1000,
        })
      );
    },
    async del(key: string) {
      await db.deleteGlobalValue(`${prefix}${key}`);
    },
  };
}

function createUtils() {
  return {
    buildUrl(base: string, query?: Record<string, string | number | boolean>) {
      const url = new URL(base);
      Object.entries(query || {}).forEach(([key, value]) => {
        url.searchParams.set(key, String(value));
      });
      return url.toString();
    },
    joinUrl(base: string, path: string) {
      return new URL(path, base).toString();
    },
    randomUA() {
      return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36';
    },
    sleep(ms: number) {
      return new Promise((resolve) => setTimeout(resolve, ms));
    },
    base64Encode(value: string) {
      return Buffer.from(value, 'utf8').toString('base64');
    },
    base64Decode(value: string) {
      return Buffer.from(value, 'base64').toString('utf8');
    },
    now() {
      return Date.now();
    },
  };
}

function createScriptFactory(code: string) {
  assertSafeCode(code);

  return new Function(
    `"use strict";
const process = undefined;
const require = undefined;
const module = undefined;
const exports = undefined;
const fetch = undefined;
${code}`
  ) as () => any;
}

async function createScriptContext(script: SourceScriptRecord, configValues?: Record<string, string>) {
  const { logs, log } = createLogCollector();
  const cache = createCacheHelpers(script.id);

  const fetcher = async (input: {
    url: string;
    method?: string;
    headers?: Record<string, string>;
    query?: Record<string, string | number | boolean>;
    body?: string;
    json?: unknown;
    timeoutMs?: number;
  }) => {
    const url = new URL(input.url);
    Object.entries(input.query || {}).forEach(([key, value]) => {
      url.searchParams.set(key, String(value));
    });

    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      throw new Error(`不支持的协议: ${url.protocol}`);
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(
      () => controller.abort(),
      input.timeoutMs || DEFAULT_TIMEOUT_MS
    );

    try {
      const response = await fetch(url.toString(), {
        method: input.method || 'GET',
        headers: {
          ...(input.json ? { 'Content-Type': 'application/json' } : {}),
          ...(input.headers || {}),
        },
        body: input.json !== undefined ? JSON.stringify(input.json) : input.body,
        signal: controller.signal,
      });

      return {
        status: response.status,
        ok: response.ok,
        url: response.url,
        headers: Object.fromEntries(response.headers.entries()),
        text: () => response.text(),
        json: <T = any>() => response.json() as Promise<T>,
        arrayBuffer: () => response.arrayBuffer(),
      };
    } finally {
      clearTimeout(timeoutId);
    }
  };

  return {
    ctx: Object.freeze({
      fetch: fetcher,
      request: {
        get: (url: string, options?: Omit<Parameters<typeof fetcher>[0], 'url' | 'method'>) =>
          fetcher({ url, method: 'GET', ...(options || {}) }),
        post: (url: string, options?: Omit<Parameters<typeof fetcher>[0], 'url' | 'method'>) =>
          fetcher({ url, method: 'POST', ...(options || {}) }),
        async getHtml(url: string, options?: Omit<Parameters<typeof fetcher>[0], 'url' | 'method'>) {
          const response = await fetcher({ url, method: 'GET', ...(options || {}) });
          const text = await response.text();
          return cheerio.load(text);
        },
        async getJson<T = any>(url: string, options?: Omit<Parameters<typeof fetcher>[0], 'url' | 'method'>) {
          const response = await fetcher({ url, method: 'GET', ...(options || {}) });
          return response.json<T>();
        },
      },
      html: {
        load: (html: string) => cheerio.load(html),
      },
      json: {
        parse<T = any>(text: string, fallback?: T) {
          try {
            return JSON.parse(text) as T;
          } catch {
            return fallback as T;
          }
        },
        stringify(value: unknown) {
          return JSON.stringify(value);
        },
      },
      utils: createUtils(),
      cache,
      log,
      config: {
        get: (key: string) => configValues?.[key],
        require: (key: string) => {
          const value = configValues?.[key];
          if (!value) {
            throw new Error(`缺少脚本配置: ${key}`);
          }
          return value;
        },
        all: () => ({ ...(configValues || {}) }),
      },
      runtime: {
        scriptId: script.id,
        sourceKey: script.key,
        sourceName: script.name,
        version: script.version,
      },
    }),
    logs,
  };
}

function normalizeScript(script: any) {
  if (!script || typeof script !== 'object') {
    throw new Error('脚本必须返回对象');
  }
  return script;
}

async function getEnabledSourceScriptByKey(key: string) {
  const registry = await loadRegistry();
  const item = registry.items.find((record) => record.key === key);
  if (!item) {
    throw new Error('脚本不存在');
  }
  if (!item.enabled) {
    throw new Error('脚本已停用');
  }
  return item;
}

async function compileSourceScript(
  script: SourceScriptRecord,
  configValues?: Record<string, string>
) {
  const factory = createScriptFactory(script.code);
  const compiled = normalizeScript(factory());
  const context = await createScriptContext(script, configValues);
  return {
    compiled,
    ...context,
  };
}

export async function executeSavedSourceScript(input: {
  key: string;
  hook: SourceScriptHook;
  payload?: Record<string, any>;
  configValues?: Record<string, string>;
}): Promise<SourceScriptTestResult> {
  const startedAt = Date.now();
  const script = await getEnabledSourceScriptByKey(input.key);
  const { compiled, ctx, logs } = await compileSourceScript(
    script,
    input.configValues
  );

  const hook = compiled[input.hook];
  if (typeof hook !== 'function') {
    throw new Error(`脚本未实现 ${input.hook} hook`);
  }

  const result = await withTimeout(
    Promise.resolve(hook(ctx, input.payload || {})),
    DEFAULT_TIMEOUT_MS
  );

  return {
    ok: true,
    durationMs: Date.now() - startedAt,
    logs,
    meta: compiled.meta,
    result,
  };
}

export async function listEnabledSourceScripts(): Promise<PublicSourceScriptSummary[]> {
  const registry = await loadRegistry();
  return registry.items
    .filter((item) => item.enabled)
    .sort((a, b) => b.updatedAt - a.updatedAt)
    .map((item) => ({
      id: item.id,
      key: item.key,
      name: item.name,
      description: item.description,
      version: item.version,
      updatedAt: item.updatedAt,
    }));
}

export async function listSourceScripts() {
  const registry = await loadRegistry();
  return registry.items.sort((a, b) => b.updatedAt - a.updatedAt);
}

export async function getSourceScript(id: string) {
  const registry = await loadRegistry();
  return registry.items.find((item) => item.id === id) || null;
}

export async function saveSourceScript(input: {
  id?: string;
  key: string;
  name: string;
  description?: string;
  code: string;
  enabled?: boolean;
}) {
  assertScriptKey(input.key);
  assertSafeCode(input.code);

  const registry = await loadRegistry();
  const now = Date.now();
  const existing = input.id
    ? registry.items.find((item) => item.id === input.id)
    : undefined;

  if (!existing && registry.items.some((item) => item.key === input.key)) {
    throw new Error('脚本 Key 已存在');
  }

  if (existing) {
    existing.history.unshift({
      version: existing.version,
      code: existing.code,
      updatedAt: existing.updatedAt,
    });
    existing.history = existing.history.slice(0, MAX_HISTORY_ITEMS);
    existing.key = input.key;
    existing.name = input.name;
    existing.description = input.description || '';
    existing.code = input.code;
    existing.enabled = input.enabled ?? existing.enabled;
    existing.updatedAt = now;
    existing.version = getNowVersion();
    await saveRegistry(registry);
    return existing;
  }

  const created: SourceScriptRecord = {
    id: nanoid(),
    key: input.key,
    name: input.name,
    description: input.description || '',
    code: input.code,
    enabled: input.enabled ?? true,
    version: getNowVersion(),
    createdAt: now,
    updatedAt: now,
    history: [],
  };

  registry.items.unshift(created);
  await saveRegistry(registry);
  return created;
}

export async function importSourceScripts(items: SourceScriptImportItem[]) {
  const registry = await loadRegistry();
  const now = Date.now();
  const imported: SourceScriptRecord[] = [];

  for (const item of items) {
    if (!item?.key || !item?.name || !item?.code) {
      throw new Error('导入脚本缺少必要字段: key/name/code');
    }

    assertScriptKey(item.key);
    assertSafeCode(item.code);

    const existing = registry.items.find((record) => record.key === item.key);

    if (existing) {
      existing.history.unshift({
        version: existing.version,
        code: existing.code,
        updatedAt: existing.updatedAt,
      });
      existing.history = existing.history.slice(0, MAX_HISTORY_ITEMS);
      existing.name = item.name;
      existing.description = item.description || '';
      existing.code = item.code;
      existing.enabled = item.enabled ?? existing.enabled;
      existing.updatedAt = now;
      existing.version = getNowVersion();
      imported.push(existing);
      continue;
    }

    const created: SourceScriptRecord = {
      id: nanoid(),
      key: item.key,
      name: item.name,
      description: item.description || '',
      code: item.code,
      enabled: item.enabled ?? true,
      version: getNowVersion(),
      createdAt: now,
      updatedAt: now,
      history: [],
    };
    registry.items.unshift(created);
    imported.push(created);
  }

  await saveRegistry(registry);
  return imported;
}

export async function deleteSourceScript(id: string) {
  const registry = await loadRegistry();
  const nextItems = registry.items.filter((item) => item.id !== id);
  if (nextItems.length === registry.items.length) {
    throw new Error('脚本不存在');
  }
  registry.items = nextItems;
  await saveRegistry(registry);
}

export async function toggleSourceScriptEnabled(id: string) {
  const registry = await loadRegistry();
  const target = registry.items.find((item) => item.id === id);
  if (!target) {
    throw new Error('脚本不存在');
  }
  target.enabled = !target.enabled;
  target.updatedAt = Date.now();
  await saveRegistry(registry);
  return target;
}

export async function restoreSourceScriptHistory(id: string, version: string) {
  const registry = await loadRegistry();
  const target = registry.items.find((item) => item.id === id);
  if (!target) {
    throw new Error('脚本不存在');
  }

  const history = target.history.find((item) => item.version === version);
  if (!history) {
    throw new Error('历史版本不存在');
  }

  target.history.unshift({
    version: target.version,
    code: target.code,
    updatedAt: target.updatedAt,
  });
  target.history = target.history.slice(0, MAX_HISTORY_ITEMS);
  target.code = history.code;
  target.version = getNowVersion();
  target.updatedAt = Date.now();
  await saveRegistry(registry);
  return target;
}

export async function testSourceScript(input: {
  code: string;
  hook: SourceScriptHook;
  payload: Record<string, any>;
  name?: string;
  key?: string;
  configValues?: Record<string, string>;
}): Promise<SourceScriptTestResult> {
  const startedAt = Date.now();
  try {
    const tempScript: SourceScriptRecord = {
      id: 'test-script',
      key: input.key || 'test-script',
      name: input.name || '测试脚本',
      description: '',
      enabled: true,
      version: 'test',
      code: input.code,
      createdAt: startedAt,
      updatedAt: startedAt,
      history: [],
    };

    const factory = createScriptFactory(input.code);
    const compiled = normalizeScript(factory());
    const hook = compiled[input.hook];
    if (typeof hook !== 'function') {
      throw new Error(`脚本未实现 ${input.hook} hook`);
    }

    const { ctx, logs } = await createScriptContext(tempScript, input.configValues);
    const result = await withTimeout(
      Promise.resolve(hook(ctx, input.payload)),
      DEFAULT_TIMEOUT_MS
    );

    return {
      ok: true,
      durationMs: Date.now() - startedAt,
      logs,
      meta: compiled.meta,
      result,
    };
  } catch (error) {
    return {
      ok: false,
      durationMs: Date.now() - startedAt,
      logs: [],
      error: (error as Error).message,
    };
  }
}

export function getDefaultSourceScriptTemplate() {
  return DEFAULT_SCRIPT_TEMPLATE;
}

export function buildScriptSourceValue(scriptKey: string, sourceId?: string) {
  return `${SCRIPT_SOURCE_PREFIX}${scriptKey}:${sourceId || 'default'}`;
}

export function parseScriptSourceValue(source: string) {
  if (!source.startsWith(SCRIPT_SOURCE_PREFIX)) {
    return null;
  }

  const rest = source.slice(SCRIPT_SOURCE_PREFIX.length);
  const separatorIndex = rest.indexOf(':');
  if (separatorIndex === -1) {
    return {
      scriptKey: rest,
      sourceId: 'default',
    };
  }

  return {
    scriptKey: rest.slice(0, separatorIndex),
    sourceId: rest.slice(separatorIndex + 1) || 'default',
  };
}

export function encodeScriptEpisodePayload(payload: Record<string, any>) {
  return `${SCRIPT_EPISODE_PREFIX}${Buffer.from(
    JSON.stringify(payload),
    'utf8'
  ).toString('base64')}`;
}

export function decodeScriptEpisodePayload(value: string) {
  if (!value.startsWith(SCRIPT_EPISODE_PREFIX)) {
    return null;
  }

  try {
    return JSON.parse(
      Buffer.from(
        value.slice(SCRIPT_EPISODE_PREFIX.length),
        'base64'
      ).toString('utf8')
    ) as Record<string, any>;
  } catch {
    return null;
  }
}

export function normalizeScriptSources(result: any): ScriptSourceDescriptor[] {
  if (!Array.isArray(result)) {
    return [{ id: 'default', name: '默认源' }];
  }

  return result
    .filter((item) => item && item.id)
    .map((item) => ({
      id: String(item.id),
      name: String(item.name || item.id),
    }));
}

export function normalizeScriptSearchResults(input: {
  scriptKey: string;
  scriptName: string;
  sourceId: string;
  sourceName: string;
  result: any;
}) {
  const list = Array.isArray(input.result?.list) ? input.result.list : [];
  return list.map((item: any) => ({
    id: String(item.id),
    title: String(item.title || ''),
    poster: item.poster || '',
    episodes: [],
    episodes_titles: [],
    source: buildScriptSourceValue(input.scriptKey, input.sourceId),
    source_name: `${input.scriptName} / ${input.sourceName}`,
    year: item.year || '',
    desc: item.desc || '',
    type_name: item.type_name || '',
    douban_id: item.douban_id || 0,
    vod_remarks: item.vod_remarks,
  }));
}

export function normalizeScriptDetailResult(input: {
  source: string;
  scriptKey: string;
  scriptName: string;
  sourceId: string;
  sourceName: string;
  detailId: string;
  result: any;
}) {
  const playbacks = Array.isArray(input.result?.playbacks)
    ? input.result.playbacks
    : [
        {
          sourceId: input.sourceId,
          sourceName: input.sourceName,
          lineId: 'default',
          lineName: '默认线路',
          episodes: input.result?.episodes || [],
          episodes_titles: input.result?.episodes_titles || [],
        },
      ];

  const flattenedEpisodes: string[] = [];
  const flattenedTitles: string[] = [];

  playbacks.forEach((playback: any) => {
    const playbackSourceId = String(playback.sourceId || input.sourceId);
    const playbackSourceName = String(playback.sourceName || input.sourceName);
    const lineId = String(playback.lineId || 'default');
    const lineName = String(playback.lineName || '默认线路');
    const titles = Array.isArray(playback.episodes_titles)
      ? playback.episodes_titles
      : [];
    const episodes = Array.isArray(playback.episodes) ? playback.episodes : [];

    episodes.forEach((episode: any, index: number) => {
      const playUrl =
        typeof episode === 'string'
          ? episode
          : String(episode?.playUrl || episode?.url || '');
      const episodeTitle =
        typeof episode === 'object' && episode?.title
          ? String(episode.title)
          : String(titles[index] || `第${index + 1}集`);

      flattenedEpisodes.push(
        encodeScriptEpisodePayload({
          script: input.scriptKey,
          sourceId: playbackSourceId,
          sourceName: playbackSourceName,
          lineId,
          lineName,
          playUrl,
          episodeIndex: index,
        })
      );
      flattenedTitles.push(`${playbackSourceName} / ${lineName} / ${episodeTitle}`);
    });
  });

  return {
    id: input.detailId,
    title: String(input.result?.title || ''),
    poster: input.result?.poster || '',
    episodes: flattenedEpisodes,
    episodes_titles: flattenedTitles,
    source: input.source,
    source_name: `${input.scriptName} / ${input.sourceName}`,
    class: input.result?.class,
    year: input.result?.year || '',
    desc: input.result?.desc || '',
    type_name: input.result?.type_name || '',
    douban_id: input.result?.douban_id || 0,
    vod_remarks: input.result?.vod_remarks,
    vod_total: input.result?.vod_total,
    proxyMode: false,
  };
}

export async function resolveScriptDetailPlaybacks(input: {
  scriptKey: string;
  sourceId: string;
  result: any;
}) {
  const playbacks = Array.isArray(input.result?.playbacks)
    ? input.result.playbacks
    : [
        {
          sourceId: input.sourceId,
          sourceName: input.sourceId,
          lineId: 'default',
          lineName: '默认线路',
          episodes: input.result?.episodes || [],
          episodes_titles: input.result?.episodes_titles || [],
        },
      ];

  const resolvedPlaybacks = await Promise.all(
    playbacks.map(async (playback: any) => {
      const playbackSourceId = String(playback.sourceId || input.sourceId);
      const lineId = String(playback.lineId || 'default');
      const episodes = Array.isArray(playback.episodes) ? playback.episodes : [];

      const resolvedEpisodes = await Promise.all(
        episodes.map(async (episode: any, index: number) => {
          const playUrl =
            typeof episode === 'string'
              ? episode
              : String(episode?.playUrl || episode?.url || '');

          try {
            const execution = await executeSavedSourceScript({
              key: input.scriptKey,
              hook: 'resolvePlayUrl',
              payload: {
                playUrl,
                sourceId: playbackSourceId,
                lineId,
                episodeIndex: index,
              },
            });

            return execution.result?.url || playUrl;
          } catch (error) {
            const message = error instanceof Error ? error.message : '';
            if (message.includes('未实现 resolvePlayUrl hook')) {
              return playUrl;
            }
            return playUrl;
          }
        })
      );

      return {
        ...playback,
        episodes: resolvedEpisodes,
      };
    })
  );

  return {
    ...input.result,
    playbacks: resolvedPlaybacks,
  };
}
