/*
 * Copyright (C) 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package y

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "badger"
	label     = "path"
)

var (
	// LSMSize has size of the LSM in bytes
	LSMSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lsm_size",
	}, []string{label})
	// VlogSize has size of the value log in bytes
	VlogSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "vlog_size",
	}, []string{label})

	// These are cumulative

	// NumReads has cumulative number of reads
	NumReads = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_reads",
	}, []string{label})
	// NumWrites has cumulative number of writes
	NumWrites = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_writes",
	}, []string{label})
	// NumBytesRead has cumulative number of bytes read
	NumBytesRead = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_bytes_read",
	}, []string{label})
	// NumVLogBytesWritten has cumulative number of bytes written
	NumVLogBytesWritten = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_bytes_written",
	}, []string{label})
	// NumLSMGets is number of LMS gets
	NumLSMGets = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_lsm_gets",
	}, []string{label})
	// NumLSMBloomHits is number of LMS bloom hits
	NumLSMBloomHits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_lsm_bloom_hits",
	}, []string{label})
	// NumGets is number of gets
	NumGets = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_gets",
	}, []string{label})
	// NumPuts is number of puts
	NumPuts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_puts",
	}, []string{label})
	// NumMemtableGets is number of memtable gets
	NumMemtableGets = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "num_memtable_gets",
	}, []string{label})

	// Histograms

	VlogSyncDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "vlog_sync_duration",
		Buckets:   prometheus.ExponentialBuckets(0.001, 1.5, 20),
	}, []string{label})

	WriteLSMDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "write_lsm_duration",
		Buckets:   prometheus.ExponentialBuckets(0.0003, 1.5, 20),
	}, []string{label})
)

type MetricsSet struct {
	LSMSize             prometheus.Gauge
	VlogSize            prometheus.Gauge
	NumReads            prometheus.Counter
	NumWrites           prometheus.Counter
	NumBytesRead        prometheus.Counter
	NumVLogBytesWritten prometheus.Counter
	NumLSMGets          prometheus.Counter
	NumLSMBloomHits     prometheus.Counter
	NumGets             prometheus.Counter
	NumPuts             prometheus.Counter
	NumMemtableGets     prometheus.Counter
	VlogSyncDuration    prometheus.Observer
	WriteLSMDuration    prometheus.Observer
}

func NewMetricSet(path string) *MetricsSet {
	return &MetricsSet{
		LSMSize:             LSMSize.WithLabelValues(path),
		VlogSize:            VlogSize.WithLabelValues(path),
		NumReads:            NumReads.WithLabelValues(path),
		NumWrites:           NumWrites.WithLabelValues(path),
		NumBytesRead:        NumBytesRead.WithLabelValues(path),
		NumVLogBytesWritten: NumVLogBytesWritten.WithLabelValues(path),
		NumLSMGets:          NumLSMGets.WithLabelValues(path),
		NumLSMBloomHits:     NumLSMBloomHits.WithLabelValues(path),
		NumGets:             NumGets.WithLabelValues(path),
		NumPuts:             NumPuts.WithLabelValues(path),
		NumMemtableGets:     NumMemtableGets.WithLabelValues(path),
		VlogSyncDuration:    VlogSyncDuration.WithLabelValues(path),
		WriteLSMDuration:    WriteLSMDuration.WithLabelValues(path),
	}
}

// These variables are global and have cumulative values for all kv stores.
func init() {
	prometheus.MustRegister(LSMSize)
	prometheus.MustRegister(VlogSize)
	prometheus.MustRegister(NumReads)
	prometheus.MustRegister(NumWrites)
	prometheus.MustRegister(NumBytesRead)
	prometheus.MustRegister(NumVLogBytesWritten)
	prometheus.MustRegister(NumLSMGets)
	prometheus.MustRegister(NumLSMBloomHits)
	prometheus.MustRegister(NumGets)
	prometheus.MustRegister(NumPuts)
	prometheus.MustRegister(NumMemtableGets)
	prometheus.MustRegister(VlogSyncDuration)
	prometheus.MustRegister(WriteLSMDuration)
}
