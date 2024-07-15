#!/usr/bin/env bun

/*
 * qryn: polyglot observability API
 * (C) 2018-2024 QXIP BV
 */

import { Router } from '@stricjs/router';
import { wrapper, wsWrapper } from './lib/bun_wrapper.js';

import {
  combinedParser,
  jsonParser,
  lokiPushJSONParser,
  lokiPushProtoParser, otlpLogsDataParser, otlpPushProtoParser, prometheusPushProtoParser,
  rawStringParser,
  tempoPushNDJSONParser,
  tempoPushParser, wwwFormParser, yamlParser
} from './parsers.js'
import handlerPush from './lib/handlers/push.js'
import handle404 from './lib/handlers/404.js'
import handlerHello from './lib/handlers/ready.js'
import handlerElasticPush from './lib/handlers/elastic_index.js'
import handlerElasticBulk from './lib/handlers/elastic_bulk.js'
import handlerTempoPush from './lib/handlers/tempo_push.js'
import handlerTempoTraces from './lib/handlers/tempo_traces.js'
import handlerTempoLabel from './lib/handlers/tempo_tags.js'
import handlerTempoLabelValues from './lib/handlers/tempo_values.js'
import handlerTempoSearch from './lib/handlers/tempo_search.js'
import handlerTempoEcho from './lib/handlers/echo.js'
import handlerTelegraf from './lib/handlers/telegraf.js'
import handlerDatadogLogPush from './lib/handlers/datadog_log_push.js'
import handlerDatadogSeriesPush from './lib/handlers/datadog_series_push.js'
import handlerQueryRange from './lib/handlers/query_range.js'
import handlerQuery from './lib/handlers/query.js'
import handlerLabel from './lib/handlers/label.js'
import handlerLabelValues from './lib/handlers/label_values.js'
import handlerSeries from './lib/handlers/series.js'
import handlerPromSeries from './lib/handlers/prom_series.js'
import promWriteHandler from './lib/handlers/prom_push.js'
import handlerPromQueryRange from './lib/handlers/prom_query_range.js'
import handlerPromQuery from './lib/handlers/prom_query.js'
import handlerPromLabel from './lib/handlers/promlabel.js'
import handlerPromLabelValues from './lib/handlers/promlabel_values.js'
import handlerPromDefault from './lib/handlers/prom_default.js'
import handlerNewrelicLogPush from './lib/handlers/newrelic_log_push.js'
import handlerInfluxWrite from './lib/handlers/influx_write.js'
import handlerInfluxHealth from './lib/handlers/influx_health.js'
import handlerOTLPPush from './lib/handlers/otlp_push.js'
import handlerGetRules from './lib/handlers/alerts/get_rules.js'
import handlerGetGroup from './lib/handlers/alerts/get_group.js'
import handlerPostGroup from './lib/handlers/alerts/post_group.js'
import handlerDelGroup from './lib/handlers/alerts/del_group.js'
import handlerDelNS from './lib/handlers/alerts/del_ns.js'
import handlerPromGetRules from './lib/handlers/alerts/prom_get_rules.js'
import handlerTail from './lib/handlers/tail.js'
import handlerTempoLabelV2 from './lib/handlers/tempo_v2_tags.js'
import handlerTempoLabelV2Values from './lib/handlers/tempo_v2_values.js'
import handlerOtlpLogsPush from './lib/handlers/otlp_log_push.js'
import {init as pyroscopeInit } from './pyroscope/pyroscope.js'

import { boolEnv, readonly, readerMode, writerMode } from './common.js'
import DATABASE, { init } from './lib/db/clickhouse.js'
import { startAlerting } from './lib/db/alerting/index.js'
import fs from 'fs'
import path from 'path'
import { file, dir, group, CORS } from '@stricjs/utils';
import auth from 'basic-auth'
import * as errors from 'http-errors'
import logger from './lib/logger.js'

const http_user = process.env.QRYN_LOGIN || process.env.CLOKI_LOGIN || undefined
const http_password = process.env.QRYN_PASSWORD || process.env.CLOKI_PASSWORD || undefined

export default async() => {
  try {
    await init(process.env.CLICKHOUSE_DB || 'cloki')
    if (process.env.MODE === 'init_only') {
      process.exit(0)
    }
  } catch (err) {
    logger.error(err, 'Error starting qryn')
    process.exit(1)
  }
  if (!readonly) {
    await startAlerting()
  }
  await DATABASE.checkDB()

  const app = new Router()

  const cors = process.env.CORS_ALLOW_ORIGIN || '*'

  app.wrap('/', (resp) => {
    const _cors = new CORS({allowOrigins: cors})
    for(const c of Object.entries(_cors.headers)) {
      resp.headers.append(c[0], c[1])
    }
    return resp
  })

  app.guard("/", (ctx) => {
    if (http_user) {
      const creds = auth({ headers: Object.fromEntries(ctx.headers.entries()) })
      if (!creds || creds.name !== http_user || creds.pass !== http_password) {
        ctx.error = new errors.Unauthorized('Unauthorized')
        return null;
      }
    }
    return ctx;
  });

  app.get('/hello', wrapper(handlerHello))
  app.get('/ready', wrapper(handlerHello))
  writerMode && app.post('/loki/api/v1/push', wrapper(handlerPush, {
      'application/json': lokiPushJSONParser,
      'application/x-protobuf': lokiPushProtoParser,
      '*': lokiPushJSONParser
    }))
  writerMode && app.post('/:target/_doc', wrapper(handlerElasticPush, {
      'application/json': jsonParser,
      '*': rawStringParser
    }))
  writerMode && app.post('/:target/_create/:id', wrapper(handlerElasticPush, {
      'application/json': jsonParser,
      '*': rawStringParser
    }))
  writerMode && app.put('/:target/_doc/:id', wrapper(handlerElasticPush, {
      'application/json': jsonParser,
      '*': rawStringParser
    }))
  writerMode && app.put('/:target/_create/:id', wrapper(handlerElasticPush, {
      'application/json': jsonParser,
      '*': rawStringParser
    }))
  writerMode && app.post('/_bulk', wrapper(handlerElasticBulk, {
      'application/json': jsonParser,
      '*': rawStringParser
    }))
  writerMode && app.post('/:target/_bulk', wrapper(handlerElasticBulk, {
      'application/json': jsonParser,
      '*': rawStringParser
    }))
  writerMode && app.post('/tempo/api/push', wrapper(handlerTempoPush, {
      'application/json': tempoPushParser,
      'application/x-ndjson': tempoPushNDJSONParser,
      '*': tempoPushParser
    }))
  writerMode && app.post('/tempo/spans', wrapper(handlerTempoPush, {
      'application/json': tempoPushParser,
      'application/x-ndjson': tempoPushNDJSONParser,
      '*': tempoPushParser
    }))
  writerMode && app.post('/api/v2/spans', wrapper(handlerTempoPush, {
      'application/json': tempoPushParser,
      'application/x-ndjson': tempoPushNDJSONParser,
      '*': tempoPushParser
    }))
  readerMode && app.get('/api/traces/:traceId', wrapper(handlerTempoTraces))
  readerMode && app.get('/api/traces/:traceId/:json', wrapper(handlerTempoTraces))
  readerMode && app.get('/tempo/api/traces/:traceId', wrapper(handlerTempoTraces))
  readerMode && app.get('/tempo/api/traces/:traceId/:json', wrapper(handlerTempoTraces))
  readerMode && app.get('/api/echo', wrapper(handlerTempoEcho))
  readerMode && app.get('/tempo/api/echo', wrapper(handlerTempoEcho))
  readerMode && app.ws('/loki/api/v1/tail', wsWrapper(handlerTail))
  app.get('/config', () => new Response('not supported'))
  app.get('/metrics', () => new Response('not supported'))
  app.get('/influx/api/v2/write/health', () => new Response('ok'))


  const fastify = {
    get: (path, hndl, parsers) => {
      app.get(path, wrapper(hndl, parsers))
    },
    post: (path, hndl, parsers) => {
      app.post(path, wrapper(hndl, parsers))
    },
    put: (path, hndl, parsers) => {
      app.put(path, wrapper(hndl, parsers))
    },
    delete: (path, hndl, parsers) => {
      app.delete(path, wrapper(hndl, parsers))
    }
  }

  readerMode && fastify.get('/api/search/tags', handlerTempoLabel)
  readerMode && fastify.get('/tempo/api/search/tags', handlerTempoLabel)

  /* Tempo Tag Value Handler */
  readerMode && fastify.get('/api/search/tag/:name/values', handlerTempoLabelValues)
  readerMode && fastify.get('/tempo/api/search/tag/:name/values', handlerTempoLabelValues)

  /* Tempo Traces Query Handler */
  readerMode && fastify.get('/api/search', handlerTempoSearch)
  readerMode && fastify.get('/tempo/api/search', handlerTempoSearch)

  /* Tempo Echo Handler */
  fastify.get('/api/echo', handlerTempoEcho)
  fastify.get('/tempo/api/echo', handlerTempoEcho)

  /* Telegraf HTTP Bulk handler */
  writerMode && fastify.post('/telegraf', handlerTelegraf, {
    '*': jsonParser
  })

  /* Datadog Log Push Handler */
  writerMode && fastify.post('/api/v2/logs', handlerDatadogLogPush, {
    'application/json': jsonParser,
    '*': rawStringParser
  })

  /* Datadog Series Push Handler */

  writerMode && fastify.post('/api/v2/series', handlerDatadogSeriesPush, {
    'application/json': jsonParser,
    '*': rawStringParser
  })

  /* Query Handler */

  readerMode && fastify.get('/loki/api/v1/query_range', handlerQueryRange)

  /* Label Handlers */
  /* Label Value Handler via query (test) */

  readerMode && fastify.get('/loki/api/v1/query', handlerQuery)

  /* Label Handlers */
  readerMode && fastify.get('/loki/api/v1/label', handlerLabel)
  readerMode && fastify.get('/loki/api/v1/labels', handlerLabel)

  /* Label Value Handler */

  readerMode && fastify.get('/loki/api/v1/label/:name/values', handlerLabelValues)

  /* Series Handler - experimental support for both Loki and Prometheus */

  readerMode && fastify.get('/loki/api/v1/series', handlerSeries)

  readerMode && fastify.get('/api/v1/series', handlerPromSeries)
  readerMode && fastify.post('/api/v1/series', handlerPromSeries, {
    'application/x-www-form-urlencoded': wwwFormParser
  })

  /* ALERT MANAGER Handlers    */
  readerMode && fastify.get('/api/prom/rules', handlerGetRules)
  readerMode && fastify.get('/api/prom/rules/:ns/:group', handlerGetGroup)
  readerMode && fastify.post('/api/prom/rules/:ns', handlerPostGroup, {
    '*': yamlParser
  })
  readerMode && fastify.delete('/api/prom/rules/:ns/:group', handlerDelGroup)
  readerMode && fastify.delete('/api/prom/rules/:ns', handlerDelNS)
  readerMode && fastify.get('/prometheus/api/v1/rules', handlerPromGetRules)

  /* PROMETHEUS REMOTE WRITE Handlers */
  const remoteWritePaths = [
    '/api/v1/prom/remote/write',
    '/api/prom/remote/write',
    '/prom/remote/write',
    '/api/v1/write',
    '/api/prom/push'
  ]
  for (const path of remoteWritePaths) {
    writerMode && fastify.post(path, promWriteHandler, {
      'application/x-protobuf': prometheusPushProtoParser,
      'application/json': jsonParser,
      '*': combinedParser(prometheusPushProtoParser, jsonParser)
    })
    writerMode && fastify.get(path, handlerTempoEcho)
  }

  /* PROMQETHEUS API EMULATION */

  readerMode && fastify.post('/api/v1/query_range', handlerPromQueryRange, {
    'application/x-www-form-urlencoded': wwwFormParser
  })
  readerMode && fastify.get('/api/v1/query_range', handlerPromQueryRange)

  readerMode && fastify.post('/api/v1/query', handlerPromQuery, {
    'application/x-www-form-urlencoded': wwwFormParser
  })
  readerMode && fastify.get('/api/v1/query', handlerPromQuery)
  readerMode && fastify.get('/api/v1/labels', handlerPromLabel) // piggyback on qryn labels
  readerMode && fastify.get('/api/v1/label/:name/values', handlerPromLabelValues) // piggyback on qryn values
  readerMode && fastify.post('/api/v1/labels', handlerPromLabel, {
    '*': rawStringParser
  }) // piggyback on qryn labels
  readerMode && fastify.post('/api/v1/label/:name/values', handlerPromLabelValues, {
    '*': rawStringParser
  }) // piggyback on qryn values

  readerMode && fastify.get('/api/v1/metadata', handlerPromDefault.misc) // default handler TBD
  readerMode && fastify.get('/api/v1/rules', handlerPromDefault.rules) // default handler TBD
  readerMode && fastify.get('/api/v1/query_exemplars', handlerPromDefault.misc) // default handler TBD
  readerMode && fastify.post('/api/v1/query_exemplars', handlerPromDefault.misc, {
    'application/x-www-form-urlencoded': wwwFormParser
  }) // default handler TBD
  readerMode && fastify.get('/api/v1/format_query', handlerPromDefault.misc) // default handler TBD
  readerMode && fastify.post('/api/v1/format_query', handlerPromDefault.misc, {
    'application/x-www-form-urlencoded': wwwFormParser
  }) // default handler TBD
  fastify.get('/api/v1/status/buildinfo', handlerPromDefault.buildinfo) // default handler TBD

  /* NewRelic Log Handler */

  writerMode && fastify.post('/log/v1', handlerNewrelicLogPush, {
    'text/plain': jsonParser,
    '*': jsonParser
  })

  /* INFLUX WRITE Handlers */

  writerMode && fastify.post('/write', handlerInfluxWrite, {
    '*': rawStringParser
  })
  writerMode && fastify.post('/influx/api/v2/write', handlerInfluxWrite, {
    '*': rawStringParser
  })
  /* INFLUX HEALTH Handlers */

  fastify.get('/health', handlerInfluxHealth)
  fastify.get('/influx/health', handlerInfluxHealth)


  writerMode && fastify.post('/v1/traces', handlerOTLPPush, {
    '*': otlpPushProtoParser
  })

  readerMode && fastify.get('/api/v2/search/tags', handlerTempoLabelV2)
  readerMode && fastify.get('/tempo/api/v2/search/tags', handlerTempoLabelV2)
  readerMode && fastify.get('/api/v2/search/tag/:name/values', handlerTempoLabelV2Values)
  readerMode && fastify.get('/tempo/api/v2/search/tag/:name/values', handlerTempoLabelV2Values)

  readerMode && pyroscopeInit(fastify)

  writerMode && fastify.post('/v1/logs', handlerOtlpLogsPush, {
    '*': otlpLogsDataParser
  })

  const serveView = fs.existsSync(path.join(__dirname, 'view/index.html'))
  if (serveView) {
    app.plug(group(path.join(__dirname, 'view')));
    for (const fakePath of ['/plugins', '/users', '/datasources', '/datasources/:ds']) {
      app.get(fakePath,
        (ctx) =>
          file(path.join(__dirname, 'view', 'index.html'))(ctx))
    }
  }

  app.use(404, (ctx) => {
    if (ctx.error && ctx.error.name === 'UnauthorizedError') {
      return new Response(ctx.error.message, {status: 401, headers: { 'www-authenticate': 'Basic' }})
    }
    return wrapper(handle404)
  })
  app.port = process.env.PORT || 3100
  app.hostname = process.env.HOST || '0.0.0.0'
  app.listen()
}
