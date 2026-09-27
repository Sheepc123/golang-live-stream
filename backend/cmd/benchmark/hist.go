package main

import (
	"math"
	"math/bits"
	"sync/atomic"
)

// hist 是一个对数-线性直方图,单位微秒,可以被几万个 goroutine 同时写。
//
// ── 为什么换掉 v1 的「1 ms 一格、上限 10 s」──
//
//  1. 上限:v1 在 5000 连接那轮 p50 就撞到 10 s 的天花板,之后的分位数全部失真。
//  2. 精度:同机压测的正常延迟在亚毫秒级,1 ms 一格会把它们全挤进第 0 格。
//
// 做法和 HdrHistogram 一样:小于 32 µs 的值每 1 µs 一格(精确);之后每个
// 2 的幂区间 [2^e, 2^(e+1)) 再均分成 32 格。于是任何值的相对误差 ≤ 1/32 ≈ 3%,
// 覆盖到 2^27 µs ≈ 134 s,总共只有 737 格(约 6 KB)。
const (
	histSubBits = 5
	histSub     = 1 << histSubBits // 每个 2 的幂区间切成 32 格
	histMaxExp  = 27               // 2^27 µs ≈ 134 s,再往上进溢出格

	// 最后一格是溢出格:≥ 2^27 µs 的值都进这里,它没有上界,只能用 max 代表。
	histOverflow = histSub + (histMaxExp-histSubBits)*histSub
	histBuckets  = histOverflow + 1
)

type hist struct {
	b     [histBuckets]atomic.Int64
	count atomic.Int64
	sum   atomic.Int64 // µs
	max   atomic.Int64 // µs
}

// bucketOf 把一个微秒值映射到格子下标。
func bucketOf(us int64) int {
	if us < histSub {
		if us < 0 {
			return 0
		}
		return int(us)
	}
	exp := bits.Len64(uint64(us)) - 1 // us ∈ [2^exp, 2^(exp+1))
	if exp >= histMaxExp {
		return histOverflow
	}
	// 最高位之后的 5 位就是它在这个 2 的幂区间里的格子序号
	mant := int(us>>(exp-histSubBits)) - histSub
	return histSub + (exp-histSubBits)*histSub + mant
}

// bucketLow 返回格子 i 的下界(含)。bucketLow(i+1) 就是它的上界(不含)。
func bucketLow(i int) int64 {
	if i < histSub {
		return int64(i)
	}
	j := i - histSub
	exp := j/histSub + histSubBits
	mant := j % histSub
	return int64(histSub+mant) << (exp - histSubBits)
}

func (h *hist) observe(us int64) {
	if us < 0 {
		// 时钟回拨之类的异常。归零而不是丢掉,保证 count 和收到的消息数一致。
		us = 0
	}
	h.b[bucketOf(us)].Add(1)
	h.count.Add(1)
	h.sum.Add(us)
	for {
		cur := h.max.Load()
		if us <= cur || h.max.CompareAndSwap(cur, us) {
			return
		}
	}
}

// quantile 返回第 q 分位的估计值(微秒):目标样本所在格子的中点,且不超过 max。
func (h *hist) quantile(q float64) float64 {
	n := h.count.Load()
	if n == 0 {
		return 0
	}
	target := max(int64(math.Ceil(q*float64(n))), 1)
	maxUs := float64(h.max.Load())

	var acc int64
	for i := range h.b {
		acc += h.b[i].Load()
		if acc < target {
			continue
		}
		if i == histOverflow {
			return maxUs
		}
		lo, hi := bucketLow(i), bucketLow(i+1)
		mid := float64(lo) + float64(hi-lo-1)/2
		return math.Min(mid, maxUs)
	}
	return maxUs
}

// Quantiles 是写进结果文件的延迟摘要,单位毫秒。
type Quantiles struct {
	Count int64   `json:"count"`
	Mean  float64 `json:"mean_ms"`
	P50   float64 `json:"p50_ms"`
	P90   float64 `json:"p90_ms"`
	P99   float64 `json:"p99_ms"`
	P999  float64 `json:"p999_ms"`
	Max   float64 `json:"max_ms"`
}

func (h *hist) summary() Quantiles {
	n := h.count.Load()
	if n == 0 {
		return Quantiles{}
	}
	return Quantiles{
		Count: n,
		Mean:  usToMs(float64(h.sum.Load()) / float64(n)),
		P50:   usToMs(h.quantile(0.50)),
		P90:   usToMs(h.quantile(0.90)),
		P99:   usToMs(h.quantile(0.99)),
		P999:  usToMs(h.quantile(0.999)),
		Max:   usToMs(float64(h.max.Load())),
	}
}

// usToMs 微秒 → 毫秒,保留两位小数(10 µs 精度,比直方图本身的误差还细)。
func usToMs(us float64) float64 { return math.Round(us/10) / 100 }
