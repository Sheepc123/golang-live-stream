package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

// 一个最小的 Prometheus 文本格式解析器 + 两次快照做差。
//
// ── 为什么要「做差」──
// 服务端的计数器是进程累计值。v1 时代每轮都得重启 server,
// 忘了重启就会把两轮的数叠在一起(docs/benchmark.md 5.1 节吃过这个亏)。
// 现在发送开始前抓一次、收尾后抓一次,两次相减就是这一轮的增量,
// 直方图的 _bucket 也是累计计数,同样可以相减后再算分位数。
//
// ── 为什么不用 prometheus/common/expfmt ──
// 这里只需要「名字 + 标签 + 值」三样东西。expfmt 的解析器 API 在最近几个版本里
// 为 UTF-8 指标名改过签名,自己解析几十行就够,也少一个随依赖升级而坏掉的点。

type promSeries struct {
	name   string
	labels map[string]string
	value  float64
}

// promSnap 以「名字 + 排序后的标签」为 key 存一次抓取的全部序列。
type promSnap map[string]promSeries

func scrapeProm(ctx context.Context, hc *http.Client, url string) (promSnap, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	// 显式要文本格式,避免内容协商拿到 protobuf / OpenMetrics
	req.Header.Set("Accept", "text/plain; version=0.0.4")
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: http %d", url, resp.StatusCode)
	}
	return parseProm(resp.Body)
}

func parseProm(r io.Reader) (promSnap, error) {
	snap := promSnap{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		if s, ok := parsePromLine(sc.Text()); ok {
			snap[s.key()] = s
		}
	}
	return snap, sc.Err()
}

// parsePromLine 解析一行 `name{k="v",...} value [timestamp]`。注释和空行返回 false。
func parsePromLine(line string) (promSeries, bool) {
	line = strings.TrimSpace(line)
	if line == "" || line[0] == '#' {
		return promSeries{}, false
	}
	i := strings.IndexAny(line, "{ ")
	if i <= 0 {
		return promSeries{}, false
	}
	s := promSeries{name: line[:i]}
	rest := line[i:]
	if rest[0] == '{' {
		labels, after, ok := parsePromLabels(rest[1:])
		if !ok {
			return promSeries{}, false
		}
		s.labels, rest = labels, after
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return promSeries{}, false
	}
	v, err := strconv.ParseFloat(fields[0], 64) // 能解析 +Inf / NaN
	if err != nil {
		return promSeries{}, false
	}
	s.value = v
	return s, true
}

// parsePromLabels 解析 `k="v",k2="v2"}` 并返回右花括号之后的剩余部分。
// 标签值里可能有转义的引号和逗号(比如 path="/a\"b"),所以不能简单按逗号切。
func parsePromLabels(s string) (map[string]string, string, bool) {
	labels := map[string]string{}
	for {
		s = strings.TrimLeft(s, " ,")
		if s == "" {
			return nil, "", false
		}
		if s[0] == '}' {
			return labels, s[1:], true
		}
		eq := strings.IndexByte(s, '=')
		if eq <= 0 || eq+1 >= len(s) || s[eq+1] != '"' {
			return nil, "", false
		}
		key := strings.TrimSpace(s[:eq])
		s = s[eq+2:]

		var b strings.Builder
		j := 0
		for ; j < len(s) && s[j] != '"'; j++ {
			if s[j] == '\\' && j+1 < len(s) {
				j++
				if s[j] == 'n' {
					b.WriteByte('\n')
				} else {
					b.WriteByte(s[j])
				}
				continue
			}
			b.WriteByte(s[j])
		}
		if j >= len(s) {
			return nil, "", false // 没有闭合的引号
		}
		labels[key] = b.String()
		s = s[j+1:]
	}
}

func (s promSeries) key() string {
	if len(s.labels) == 0 {
		return s.name
	}
	var b strings.Builder
	b.WriteString(s.name)
	b.WriteByte('{')
	for i, k := range slices.Sorted(maps.Keys(s.labels)) {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s=%q", k, s.labels[k])
	}
	b.WriteByte('}')
	return b.String()
}

// match 检查标签是否满足 kv(按 key, value, key, value… 成对给出)。
func (s promSeries) match(kv []string) bool {
	for i := 0; i+1 < len(kv); i += 2 {
		if s.labels[kv[i]] != kv[i+1] {
			return false
		}
	}
	return true
}

// sub 返回 after - before。只对计数器和直方图有意义;gauge 请直接读 after。
// before 里没有的序列(这一轮才第一次出现的标签组合)按 0 处理。
func (after promSnap) sub(before promSnap) promSnap {
	out := make(promSnap, len(after))
	for k, s := range after {
		s.value -= before[k].value
		out[k] = s
	}
	return out
}

// total 对名字相同、标签满足 kv 的所有序列求和。
func (s promSnap) total(name string, kv ...string) float64 {
	v, _ := s.value(name, kv...)
	return v
}

// value 和 total 一样,但额外告诉调用方「这个指标到底存在不存在」——
// 老版本的 server 没有新加的指标,报告里要显示 "-" 而不是 0。
func (s promSnap) value(name string, kv ...string) (float64, bool) {
	var sum float64
	found := false
	for _, x := range s {
		if x.name == name && x.match(kv) {
			sum += x.value
			found = true
		}
	}
	return sum, found
}

// histQuantile 和 PromQL 的 histogram_quantile 算法相同:
// 找到第 q 分位落在哪个桶,在桶的上下界之间线性插值。
// name 不带 _bucket 后缀。精度受桶宽限制,只能当估计值看。
func (s promSnap) histQuantile(name string, q float64, kv ...string) (float64, bool) {
	cum := map[float64]float64{}
	for _, x := range s {
		if x.name != name+"_bucket" || !x.match(kv) {
			continue
		}
		le, err := strconv.ParseFloat(x.labels["le"], 64)
		if err != nil {
			continue
		}
		cum[le] += x.value // 多个标签组合(比如不同 type)的同一个 le 加在一起
	}
	if len(cum) == 0 {
		return 0, false
	}
	les := slices.Sorted(maps.Keys(cum))
	total := cum[les[len(les)-1]] // 最后一个是 +Inf 桶 = 样本总数
	if total <= 0 {
		return 0, false
	}

	rank := q * total
	prevLe, prevCum := 0.0, 0.0
	for _, le := range les {
		c := cum[le]
		if c >= rank {
			if math.IsInf(le, 1) {
				return prevLe, true // 落在 +Inf 桶:只能报最高的有限上界
			}
			if c == prevCum {
				return le, true
			}
			return prevLe + (le-prevLe)*(rank-prevCum)/(c-prevCum), true
		}
		prevLe, prevCum = le, c
	}
	return prevLe, true
}
