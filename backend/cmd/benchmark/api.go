package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// 压测房间的标题前缀。prepareRooms 会优先复用 admin 名下带这个前缀的房间,
// 不够才新建 —— v1 每跑一次就建 N 个新房间,数据库里越积越多。
const benchRoomPrefix = "bench-room-"

// 服务端统一响应壳:{code, data, msg}。
type apiResponse struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

func apiCall(hc *http.Client, method, rawURL, token string, body any, out any) error {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return err
		}
	}
	req, err := http.NewRequest(method, rawURL, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var env apiResponse
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return fmt.Errorf("%s %s: http %d, 响应不是 JSON: %w", method, rawURL, resp.StatusCode, err)
	}
	if env.Code != 0 {
		return fmt.Errorf("%s %s: http %d, code=%d msg=%s", method, rawURL, resp.StatusCode, env.Code, env.Msg)
	}
	if out != nil {
		return json.Unmarshal(env.Data, out)
	}
	return nil
}

// login 登录拿 access_token;账号不存在就先注册再登录。
func login(hc *http.Client, base, user, pass string) (string, error) {
	var out struct {
		AccessToken string `json:"access_token"`
	}
	cred := map[string]string{"username": user, "password": pass}

	if err := apiCall(hc, http.MethodPost, base+"/api/v1/auth/login", "", cred, &out); err == nil {
		return out.AccessToken, nil
	}
	fmt.Printf("登录 %s 失败,尝试注册\n", user)
	if err := apiCall(hc, http.MethodPost, base+"/api/v1/auth/register", "", cred, nil); err != nil {
		return "", fmt.Errorf("注册失败: %w", err)
	}
	if err := apiCall(hc, http.MethodPost, base+"/api/v1/auth/login", "", cred, &out); err != nil {
		return "", fmt.Errorf("注册后登录仍失败: %w", err)
	}
	return out.AccessToken, nil
}

// prepareRooms 准备 n 个房间并开播。
// reuse 非空时直接用这些 id;否则复用 admin 名下的 bench-room-*,不够再建。
// 开播接口是幂等的:房间已在播会直接返回当前场次,所以反复跑不会报错。
func prepareRooms(hc *http.Client, base, token string, n int, reuse string) ([]int64, error) {
	var ids []int64
	if reuse != "" {
		for _, s := range strings.Split(reuse, ",") {
			id, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("-room-ids 里 %q 不是整数", s)
			}
			ids = append(ids, id)
		}
	} else {
		var mine struct {
			Rooms []struct {
				ID    int64  `json:"id"`
				Title string `json:"title"`
			} `json:"rooms"`
		}
		if err := apiCall(hc, http.MethodGet, base+"/api/v1/rooms/mine", token, nil, &mine); err != nil {
			return nil, fmt.Errorf("查询已有房间失败: %w", err)
		}
		for _, r := range mine.Rooms {
			if strings.HasPrefix(r.Title, benchRoomPrefix) {
				ids = append(ids, r.ID)
			}
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		if len(ids) > n {
			ids = ids[:n]
		}
		for len(ids) < n {
			var room struct {
				ID int64 `json:"id"`
			}
			body := map[string]string{
				"title":       fmt.Sprintf("%s%d", benchRoomPrefix, len(ids)),
				"anchor_name": "bench",
				"category":    "bench",
			}
			if err := apiCall(hc, http.MethodPost, base+"/api/v1/rooms", token, body, &room); err != nil {
				return nil, fmt.Errorf("创建房间失败: %w", err)
			}
			ids = append(ids, room.ID)
		}
	}

	for _, id := range ids {
		u := fmt.Sprintf("%s/api/v1/rooms/%d/live/start", base, id)
		if err := apiCall(hc, http.MethodPost, u, token, nil, nil); err != nil {
			return nil, fmt.Errorf("房间 %d 开播失败: %w", id, err)
		}
	}
	return ids, nil
}
