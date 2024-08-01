const { parseTypeId } = require('./shared')
const { mergeStackTraces } = require('./merge_stack_traces')

const render = async (req, res) => {
  const query = req.query.query
  const parsedQuery = parseQuery(query)
  const fromTimeSec = req.query.from
    ? Math.floor(parseInt(req.query.from) / 1000)
    : Math.floor((Date.now() - 1000 * 60 * 60 * 48) / 1000)
  const toTimeSec = req.query.until
    ? Math.floor(parseInt(req.query.from) / 1000)
    : Math.floor((Date.now() - 1000 * 60 * 60 * 48) / 1000)
  if (!parsedQuery) {
    return res.sendStatus(400).send('Invalid query')
  }
  const groupBy = req.query.groupBy
  let agg = ''
  switch (req.query.aggregation) {
    case 'sum':
      agg = 'sum'
      break
    case 'avg':
      agg = 'avg'
      break
  }
  if (req.query.format === 'dot') {
    return res.sendStatus(400).send('Dot format is not supported')
  }
  const mergeStackTrace = mergeStackTraces(
    parsedQuery.typeDesc,
    parsedQuery.labelSelector,
    fromTimeSec,
    toTimeSec,
    req.log)
  //TODO
}

/*
func (q *QueryHandlers) Render(w http.ResponseWriter, req *http.Request) {
	var resFlame *connect.Response[querierv1.SelectMergeStacktracesResponse]
	g, ctx := errgroup.WithContext(req.Context())
	selectParamsClone := selectParams.CloneVT()
	g.Go(func() error {
		var err error
		resFlame, err = q.client.SelectMergeStacktraces(ctx, connect.NewRequest(selectParamsClone))
		return err
	})

	timelineStep := timeline.CalcPointInterval(selectParams.Start, selectParams.End)
	var resSeries *connect.Response[querierv1.SelectSeriesResponse]
	g.Go(func() error {
		var err error
		resSeries, err = q.client.SelectSeries(req.Context(),
			connect.NewRequest(&querierv1.SelectSeriesRequest{
				ProfileTypeID: selectParams.ProfileTypeID,
				LabelSelector: selectParams.LabelSelector,
				Start:         selectParams.Start,
				End:           selectParams.End,
				Step:          timelineStep,
				GroupBy:       groupBy,
				Aggregation:   &aggregation,
			}))

		return err
	})

	err = g.Wait()
	if err != nil {
		httputil.Error(w, err)
		return
	}

	seriesVal := &typesv1.Series{}
	if len(resSeries.Msg.Series) == 1 {
		seriesVal = resSeries.Msg.Series[0]
	}

	fb := phlaremodel.ExportToFlamebearer(resFlame.Msg.Flamegraph, profileType)
	fb.Timeline = timeline.New(seriesVal, selectParams.Start, selectParams.End, int64(timelineStep))

	if len(groupBy) > 0 {
		fb.Groups = make(map[string]*flamebearer.FlamebearerTimelineV1)
		for _, s := range resSeries.Msg.Series {
			key := "*"
			for _, l := range s.Labels {
				// right now we only support one group by
				if l.Name == groupBy[0] {
					key = l.Value
					break
				}
			}
			fb.Groups[key] = timeline.New(s, selectParams.Start, selectParams.End, int64(timelineStep))
		}
	}

	w.Header().Add("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(fb); err != nil {
		httputil.Error(w, err)
		return
	}
}
 */

/**
 *
 * @param query {string}
 */
const parseQuery = (query) => {
  query = query.trim()
  const match = query.match(/^([^{]+)\s*(\{(.*)})?$/)
  if (!match) {
    return null
  }
  const typeId = match[1]
  const typeDesc = parseTypeId(typeId)
  const strLabels = match[3] || ''
  const labels = []
  if (strLabels && strLabels !== '') {
    for (const m in strLabels.matchAll(/([,{])\s*([^A-Za-z0-9_]+)\s*(!=|!~|=~|=)\s*("([^"\\]|\\.)*")/g)) {
      labels.push([m[2], m[3], m[4]])
    }
  }
  return {
    typeId,
    typeDesc,
    labels,
    labelSelector: strLabels
  }
}

const init = (fastify) => {
  fastify.get('/pyroscope/render', render)
}

module.exports = {
  init
}
