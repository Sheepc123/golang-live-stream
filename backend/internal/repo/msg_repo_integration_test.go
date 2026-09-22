package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/config"
	"github.com/Sheepc123/golang-live-stream/internal/model/entity"
	drivermysql "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// mysqlFixture 是一条独占的 MySQL 连接 + 一张会话级临时表。
//
// 临时表 LIKE messages 建出来,所以它带着真实的索引和列定义,
// 但断开连接就消失,永远不会碰到 messages 本身。
type mysqlFixture struct {
	ctx   context.Context
	conn  *sql.Conn
	db    *gorm.DB
	table string
	repo  MsgRepo
}

// newMySQLFixture 默认连配置文件里的开发库并只读检查它的 schema;
// 设了 MYSQL_TEST_SOCKET 则连一个一次性的 fixture 实例并自己建表。
func newMySQLFixture(t *testing.T) *mysqlFixture {
	t.Helper()
	if os.Getenv("RUN_MYSQL_INTEGRATION") != "1" {
		t.Skip("set RUN_MYSQL_INTEGRATION=1 to verify the configured MySQL database")
	}
	t.Chdir("../..")

	fixtureSocket := os.Getenv("MYSQL_TEST_SOCKET")
	dsn := drivermysql.NewConfig()
	if fixtureSocket != "" {
		dsn.User, dsn.Net, dsn.Addr = "root", "unix", fixtureSocket
		dsn.DBName, dsn.ParseTime = "consumer_batch_test", true
	} else {
		cfg, err := config.Load("configs/config.yaml")
		if err != nil {
			t.Fatal("cannot load database configuration (details suppressed to protect credentials)")
		}
		dsn, err = drivermysql.ParseDSN(cfg.MySQL.DSN())
		if err != nil {
			t.Fatal("invalid database DSN")
		}
	}
	dsn.Timeout = 5 * time.Second
	dsn.ReadTimeout = 5 * time.Second
	dsn.WriteTimeout = 5 * time.Second

	pool, err := sql.Open("mysql", dsn.FormatDSN())
	if err != nil {
		t.Fatal("cannot create database connection")
	}
	t.Cleanup(func() { _ = pool.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	conn, err := pool.Conn(ctx)
	if err != nil {
		t.Fatalf("cannot connect to configured MySQL: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	db, err := gorm.Open(mysql.New(mysql.Config{Conn: conn}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	if fixtureSocket != "" {
		if err := db.Set("gorm:table_options", "ENGINE=InnoDB").AutoMigrate(&entity.Message{}); err != nil {
			t.Fatal(err)
		}
		t.Log("using disposable MySQL fixture; this does not verify the project's existing database schema")
	}

	var engine string
	if err := conn.QueryRowContext(ctx, "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'messages'").Scan(&engine); err != nil {
		t.Fatalf("inspect messages engine: %v", err)
	}
	if !strings.EqualFold(engine, "InnoDB") {
		t.Fatalf("messages engine = %q, want InnoDB", engine)
	}
	var uniqueIndexes int
	err = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM (
		SELECT INDEX_NAME FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'messages' AND NON_UNIQUE = 0
		GROUP BY INDEX_NAME
		HAVING COUNT(*) = 1 AND MAX(COLUMN_NAME) = 'event_id' AND MAX(SUB_PART) IS NULL
	) AS event_indexes`).Scan(&uniqueIndexes)
	if err != nil {
		t.Fatalf("inspect event_id index: %v", err)
	}
	if uniqueIndexes == 0 {
		t.Fatal("messages is missing a full-column unique event_id index")
	}
	t.Log("connected messages schema: InnoDB and full-column unique event_id index verified")

	// 每个测试一张自己的临时表,表名带上测试名避免同一连接上互相影响。
	table := "msg_repo_test_" + strings.ToLower(strings.ReplaceAll(t.Name(), "/", "_"))
	if _, err := conn.ExecContext(ctx, "CREATE TEMPORARY TABLE "+table+" LIKE messages"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := conn.ExecContext(cleanupCtx, "DROP TEMPORARY TABLE "+table); err != nil {
			t.Errorf("temporary table cleanup: %v", err)
		}
	})
	// 只在这条测试连接上把 sql_mode 收紧,让「数据太长」是错误而不是静默截断。
	if _, err := conn.ExecContext(ctx, "SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION'"); err != nil {
		t.Fatal(err)
	}

	return &mysqlFixture{
		ctx:   ctx,
		conn:  conn,
		db:    db,
		table: table,
		repo:  NewMesRep(db.Table(table)),
	}
}

func (f *mysqlFixture) count(t *testing.T) int64 {
	t.Helper()
	var got int64
	if err := f.db.Table(f.table).Count(&got).Error; err != nil {
		t.Fatal(err)
	}
	return got
}

func (f *mysqlFixture) assertCount(t *testing.T, want int64) {
	t.Helper()
	if got := f.count(t); got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
}

func testMessage(eventID, content string) entity.Message {
	return entity.Message{
		EventID: eventID, RoomID: 1, UserID: 1, Username: "batch-test",
		Content: content, Type: "chat", LiveSessionID: 1, SentAt: time.Now().UnixMilli(),
	}
}

func TestCreateBatchIfAbsentMySQL(t *testing.T) {
	f := newMySQLFixture(t)
	r, ctx := f.repo, f.ctx

	batch := []entity.Message{testMessage("test-A", "original-A"), testMessage("test-B", "original-B")}
	if err := r.CreateBatchIfAbsent(ctx, batch); err != nil {
		t.Fatal(err)
	}
	f.assertCount(t, 2)
	// Reuse the slice after GORM has had a chance to populate generated IDs.
	if err := r.CreateBatchIfAbsent(ctx, batch); err != nil {
		t.Fatal(err)
	}
	f.assertCount(t, 2)
	if err := r.CreateBatchIfAbsent(ctx, []entity.Message{testMessage("test-B", "must-not-overwrite"), testMessage("test-C", "new-C")}); err != nil {
		t.Fatal(err)
	}
	f.assertCount(t, 3)
	var content string
	if err := f.conn.QueryRowContext(ctx, "SELECT content FROM "+f.table+" WHERE event_id = ?", "test-B").Scan(&content); err != nil {
		t.Fatal(err)
	}
	if content != "original-B" {
		t.Fatalf("duplicate overwrote content: %q", content)
	}
	t.Log("A/B insert, replay, B/C mixed batch and original content preservation verified")

	// A deterministic data error must leave no valid prefix of the batch behind.
	invalid := testMessage("invalid", "invalid-row")
	invalid.Username = strings.Repeat("x", 10000)
	err := r.CreateBatchIfAbsent(ctx, []entity.Message{testMessage("rollback-A", "valid"), invalid})
	var mysqlErr *drivermysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1406 {
		t.Fatalf("expected data-too-long error 1406, got %v", err)
	}
	f.assertCount(t, 3)
	t.Log("invalid row rolls back the batch")

	// Verify the repository transaction spans multiple INSERT statements too.
	large := make([]entity.Message, messageInsertBatchSize+1)
	for i := range messageInsertBatchSize {
		large[i] = testMessage(fmt.Sprintf("large-%d", i), "valid")
	}
	large[messageInsertBatchSize] = invalid
	if err := r.CreateBatchIfAbsent(ctx, large); !errors.As(err, &mysqlErr) || mysqlErr.Number != 1406 {
		t.Fatalf("expected second-statement data error, got %v", err)
	}
	f.assertCount(t, 3)
	if err := r.CreateBatchIfAbsent(ctx, large[:messageInsertBatchSize]); err != nil {
		t.Fatal(err)
	}
	f.assertCount(t, 503)
	if err := r.CreateBatchIfAbsent(ctx, large[:messageInsertBatchSize]); err != nil {
		t.Fatal(err)
	}
	f.assertCount(t, 503)
	t.Log("500-row batch succeeds and replay adds no duplicates")
	if err := r.CreateBatchIfAbsent(ctx, nil); err != nil {
		t.Fatal(err)
	}
	t.Log("second INSERT failure rolls back the first 500 rows; empty batch succeeds")
}

// 历史消息的顺序由 sent_at 决定,不由插入顺序(id)决定;
// sent_at 相同时才退回 id。limit 截断的是「最旧」的那头,返回仍是升序。
//
// 这条保证是 Kafka 分区键能自由选择的前提:
// 不管消息从哪个分区、以什么顺序落库,读出来的先后都一样。
func TestListBySessionIDOrdersBySentAtThenIDMySQL(t *testing.T) {
	f := newMySQLFixture(t)
	r, ctx := f.repo, f.ctx

	at := func(eventID string, sentAt int64) entity.Message {
		m := testMessage(eventID, eventID)
		m.SentAt = sentAt
		return m
	}

	// 一条一条插,让自增 id 严格等于插入顺序,和 sent_at 故意错开。
	//   插入顺序: A(300) B(100) C(200) D(300)
	//   期望顺序: B(100) C(200) A(300) D(300)   ← A、D 同 sent_at,按 id
	inserts := []entity.Message{
		at("order-A", 300),
		at("order-B", 100),
		at("order-C", 200),
		at("order-D", 300),
	}
	for i := range inserts {
		if err := r.CreateBatchIfAbsent(ctx, inserts[i:i+1]); err != nil {
			t.Fatal(err)
		}
	}

	// 干扰项:别的房间、别的场次,sent_at 更早,绝不能混进来。
	otherRoom := at("order-other-room", 50)
	otherRoom.RoomID = 2
	otherSession := at("order-other-session", 50)
	otherSession.LiveSessionID = 2
	if err := r.CreateBatchIfAbsent(ctx, []entity.Message{otherRoom, otherSession}); err != nil {
		t.Fatal(err)
	}
	f.assertCount(t, 6)

	assertOrder := func(limit int, want ...string) {
		t.Helper()
		got, err := r.ListBySessionID(ctx, 1, 1, limit)
		if err != nil {
			t.Fatalf("ListBySessionID(limit=%d): %v", limit, err)
		}
		ids := make([]string, len(got))
		for i := range got {
			ids[i] = got[i].EventID
		}
		if strings.Join(ids, ",") != strings.Join(want, ",") {
			t.Fatalf("ListBySessionID(limit=%d) = %v, want %v", limit, ids, want)
		}
		for i := 1; i < len(got); i++ {
			prev, cur := got[i-1], got[i]
			if cur.SentAt < prev.SentAt || (cur.SentAt == prev.SentAt && cur.ID <= prev.ID) {
				t.Fatalf("ListBySessionID(limit=%d) not ascending by (sent_at, id) at index %d: %+v then %+v", limit, i, prev, cur)
			}
		}
	}

	assertOrder(10, "order-B", "order-C", "order-A", "order-D")
	// limit 从「最新」那头取,取出来仍然升序
	assertOrder(3, "order-C", "order-A", "order-D")
	assertOrder(2, "order-A", "order-D")
	assertOrder(1, "order-D")
	t.Log("ascending by sent_at then id; limit keeps the newest rows")
}
