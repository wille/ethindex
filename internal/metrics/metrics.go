// Package metrics exposes Prometheus metrics for ethindex.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Transaction lifecycle transitions, the core business metric.
	transactionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ethindex_transactions_total",
			Help: "Total number of transaction lifecycle transitions",
		},
		[]string{"indexer", "status", "direction"},
	)

	// Detection-to-finality latency of confirmed payments.
	confirmationLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ethindex_confirmation_latency_seconds",
			Help:    "Time from first observation of a transaction to its confirmation",
			Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 1200, 3600},
		},
		[]string{"indexer"},
	)

	// Chain progress: alert when head minus last processed grows.
	lastProcessedBlock = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ethindex_last_processed_block",
			Help: "Highest block fully processed",
		},
		[]string{"indexer"},
	)

	chainHeadBlock = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ethindex_chain_head_block",
			Help: "Latest block number announced by the node",
		},
		[]string{"indexer"},
	)

	finalizedBlock = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ethindex_finalized_block",
			Help: "Node-reported finalized/safe block number (tag mode only)",
		},
		[]string{"indexer"},
	)

	trackedTransactions = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ethindex_tracked_transactions",
			Help: "Transactions currently tracked in flight (pending or awaiting confirmation)",
		},
		[]string{"indexer"},
	)

	blocksProcessedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ethindex_blocks_processed_total",
			Help: "Total number of blocks scanned",
		},
		[]string{"indexer"},
	)

	blockProcessDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ethindex_block_process_duration_seconds",
			Help:    "Time to fully process one block",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"indexer"},
	)

	catchupRemainingBlocks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ethindex_catchup_remaining_blocks",
			Help: "Blocks left in the currently running catch-up (0 when caught up)",
		},
		[]string{"indexer"},
	)

	reorgsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ethindex_reorgs_total",
			Help: "Total number of chain reorganizations handled",
		},
		[]string{"indexer"},
	)

	reorgDepth = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ethindex_reorg_depth",
			Help:    "Number of previously canonical blocks reverted per reorg",
			Buckets: []float64{1, 2, 3, 5, 10, 25, 50, 100},
		},
		[]string{"indexer"},
	)

	sessionReconnectsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ethindex_session_reconnects_total",
			Help: "Total number of node session reconnects",
		},
		[]string{"indexer"},
	)

	pendingDroppedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ethindex_pending_notifications_dropped_total",
			Help: "Pending-transaction notifications dropped due to overload",
		},
		[]string{"indexer"},
	)
)

// Handler returns an HTTP handler for the Prometheus metrics endpoint.
func Handler() http.Handler {
	prometheus.MustRegister(transactionsTotal)
	prometheus.MustRegister(confirmationLatency)
	prometheus.MustRegister(lastProcessedBlock)
	prometheus.MustRegister(chainHeadBlock)
	prometheus.MustRegister(finalizedBlock)
	prometheus.MustRegister(trackedTransactions)
	prometheus.MustRegister(blocksProcessedTotal)
	prometheus.MustRegister(blockProcessDuration)
	prometheus.MustRegister(catchupRemainingBlocks)
	prometheus.MustRegister(reorgsTotal)
	prometheus.MustRegister(reorgDepth)
	prometheus.MustRegister(sessionReconnectsTotal)
	prometheus.MustRegister(pendingDroppedTotal)
	return promhttp.Handler()
}

// RecordTransaction records one lifecycle transition.
func RecordTransaction(indexer, status, direction string) {
	transactionsTotal.WithLabelValues(indexer, status, direction).Inc()
}

// RecordConfirmationLatency records detection-to-finality time.
func RecordConfirmationLatency(indexer string, d time.Duration) {
	confirmationLatency.WithLabelValues(indexer).Observe(d.Seconds())
}

func SetLastProcessedBlock(indexer string, number uint64) {
	lastProcessedBlock.WithLabelValues(indexer).Set(float64(number))
}

func SetChainHeadBlock(indexer string, number uint64) {
	chainHeadBlock.WithLabelValues(indexer).Set(float64(number))
}

func SetFinalizedBlock(indexer string, number uint64) {
	finalizedBlock.WithLabelValues(indexer).Set(float64(number))
}

func SetTrackedTransactions(indexer string, n int) {
	trackedTransactions.WithLabelValues(indexer).Set(float64(n))
}

// RecordBlockProcessed records one fully processed block.
func RecordBlockProcessed(indexer string, d time.Duration) {
	blocksProcessedTotal.WithLabelValues(indexer).Inc()
	blockProcessDuration.WithLabelValues(indexer).Observe(d.Seconds())
}

func SetCatchupRemainingBlocks(indexer string, n uint64) {
	catchupRemainingBlocks.WithLabelValues(indexer).Set(float64(n))
}

// RecordReorg records one handled reorganization and its depth.
func RecordReorg(indexer string, depth uint64) {
	reorgsTotal.WithLabelValues(indexer).Inc()
	reorgDepth.WithLabelValues(indexer).Observe(float64(depth))
}

func RecordSessionReconnect(indexer string) {
	sessionReconnectsTotal.WithLabelValues(indexer).Inc()
}

func RecordPendingDropped(indexer string, n uint64) {
	pendingDroppedTotal.WithLabelValues(indexer).Add(float64(n))
}
