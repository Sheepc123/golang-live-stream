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

// By default, inspect the configured development schema without modifying it.
// MYSQL_TEST_SOCKET selects a disposable fixture server and creates its schema.
// Repository writes always target a connection-local temporary table.
func TestCreateBatchIfAbsentMySQL(t *testing.T) {
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
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pool.Conn(ctx)
	if err != nil {
		t.Fatalf("cannot connect to configured MySQL: %v", err)
	}
	defer conn.Close()
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

	// The temporary table disappears on disconnect and never changes messages.
	const table = "consumer_batch_verification"
	if _, err := conn.ExecContext(ctx, "CREATE TEMPORARY TABLE "+table+" LIKE messages"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := conn.ExecContext(cleanupCtx, "DROP TEMPORARY TABLE "+table); err != nil {
			t.Errorf("temporary table cleanup: %v", err)
		}
	}()
	// Enforce a known error policy only on this dedicated test connection.
	if _, err := conn.ExecContext(ctx, "SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION'"); err != nil {
		t.Fatal(err)
	}
	r := NewMesRep(db.Table(table))
	message := func(id, content string) entity.Message {
		return entity.Message{EventID: id, RoomID: 1, UserID: 1, Username: "batch-test", Content: content, Type: "chat", LiveSessionID: 1, SentAt: time.Now().UnixMilli()}
	}
	assertCount := func(want int64) {
		t.Helper()
		var got int64
		if err := db.Table(table).Count(&got).Error; err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("rows = %d, want %d", got, want)
		}
	}
	batch := []entity.Message{message("test-A", "original-A"), message("test-B", "original-B")}
	if err := r.CreateBatchIfAbsent(ctx, batch); err != nil {
		t.Fatal(err)
	}
	assertCount(2)
	// Reuse the slice after GORM has had a chance to populate generated IDs.
	if err := r.CreateBatchIfAbsent(ctx, batch); err != nil {
		t.Fatal(err)
	}
	assertCount(2)
	if err := r.CreateBatchIfAbsent(ctx, []entity.Message{message("test-B", "must-not-overwrite"), message("test-C", "new-C")}); err != nil {
		t.Fatal(err)
	}
	assertCount(3)
	var content string
	if err := conn.QueryRowContext(ctx, "SELECT content FROM "+table+" WHERE event_id = ?", "test-B").Scan(&content); err != nil {
		t.Fatal(err)
	}
	if content != "original-B" {
		t.Fatalf("duplicate overwrote content: %q", content)
	}
	t.Log("A/B insert, replay, B/C mixed batch and original content preservation verified")

	// A deterministic data error must leave no valid prefix of the batch behind.
	invalid := message("invalid", "invalid-row")
	invalid.Username = strings.Repeat("x", 10000)
	err = r.CreateBatchIfAbsent(ctx, []entity.Message{message("rollback-A", "valid"), invalid})
	var mysqlErr *drivermysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1406 {
		t.Fatalf("expected data-too-long error 1406, got %v", err)
	}
	assertCount(3)
	t.Log("invalid row rolls back the batch")

	// Verify the repository transaction spans multiple INSERT statements too.
	large := make([]entity.Message, messageInsertBatchSize+1)
	for i := 0; i < messageInsertBatchSize; i++ {
		large[i] = message(fmt.Sprintf("large-%d", i), "valid")
	}
	large[messageInsertBatchSize] = invalid
	if err := r.CreateBatchIfAbsent(ctx, large); !errors.As(err, &mysqlErr) || mysqlErr.Number != 1406 {
		t.Fatalf("expected second-statement data error, got %v", err)
	}
	assertCount(3)
	if err := r.CreateBatchIfAbsent(ctx, large[:messageInsertBatchSize]); err != nil {
		t.Fatal(err)
	}
	assertCount(503)
	if err := r.CreateBatchIfAbsent(ctx, large[:messageInsertBatchSize]); err != nil {
		t.Fatal(err)
	}
	assertCount(503)
	t.Log("500-row batch succeeds and replay adds no duplicates")
	if err := r.CreateBatchIfAbsent(ctx, nil); err != nil {
		t.Fatal(err)
	}
	t.Log("second INSERT failure rolls back the first 500 rows; empty batch succeeds")
}
