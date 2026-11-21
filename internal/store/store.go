// Package store 是 MySQL 存取層（database/sql，不用 ORM，查詢和索引看得一清二楚）。
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Chunweiwu518/vod-gateway/internal/cache"
	_ "github.com/go-sql-driver/mysql"
)

//go:embed schema.sql
var schema string

type Video struct {
	ID          int64     `json:"id"`
	CreatorID   int64     `json:"creator_id"`
	Title       string    `json:"title"`
	Description string    `json:"description"`
	DurationSec int       `json:"duration_sec"`
	PlayCount   int64     `json:"play_count"`
	FavCount    int64     `json:"fav_count"`
	CreatedAt   time.Time `json:"created_at"`
}

type Creator struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Followers int64     `json:"followers"`
	CreatedAt time.Time `json:"created_at"`
}

type HistoryItem struct {
	VideoID     int64     `json:"video_id"`
	Title       string    `json:"title"`
	PositionSec int       `json:"position_sec"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type PlayEvent struct {
	UserID      int64
	VideoID     int64
	PositionSec int
}

type Store struct{ DB *sql.DB }

func Open(dsn string) (*Store, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(64)
	db.SetMaxIdleConns(64)
	db.SetConnMaxLifetime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		if err = db.PingContext(ctx); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("mysql not ready: %w", err)
		case <-time.After(time.Second):
		}
	}
	return &Store{DB: db}, nil
}

// Migrate 建表（IF NOT EXISTS，可重複執行）。
func (s *Store) Migrate(ctx context.Context) error {
	for _, stmt := range strings.Split(schema, ";\n") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := s.DB.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

const videoCols = `id, creator_id, title, description, duration_sec, play_count, fav_count, created_at`

func scanVideo(sc interface{ Scan(...any) error }) (Video, error) {
	var v Video
	err := sc.Scan(&v.ID, &v.CreatorID, &v.Title, &v.Description, &v.DurationSec, &v.PlayCount, &v.FavCount, &v.CreatedAt)
	return v, err
}

func (s *Store) GetVideo(ctx context.Context, id int64) (Video, error) {
	v, err := scanVideo(s.DB.QueryRowContext(ctx, `SELECT `+videoCols+` FROM videos WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return v, cache.ErrNotFound
	}
	return v, err
}

func (s *Store) GetCreator(ctx context.Context, id int64) (Creator, error) {
	var c Creator
	err := s.DB.QueryRowContext(ctx, `SELECT id, name, followers, created_at FROM creators WHERE id = ?`, id).
		Scan(&c.ID, &c.Name, &c.Followers, &c.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return c, cache.ErrNotFound
	}
	return c, err
}

func (s *Store) ListByCreator(ctx context.Context, creatorID int64, limit int) ([]Video, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT `+videoCols+` FROM videos WHERE creator_id = ? ORDER BY created_at DESC, id DESC LIMIT ?`, creatorID, limit)
	if err != nil {
		return nil, err
	}
	return collectVideos(rows)
}

func collectVideos(rows *sql.Rows) ([]Video, error) {
	defer rows.Close()
	out := []Video{}
	for rows.Next() {
		v, err := scanVideo(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// SearchVideos 用 ngram 全文索引（中文／英文都能搜，最小詞長 2）。
func (s *Store) SearchVideos(ctx context.Context, q string, limit int) ([]Video, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT `+videoCols+` FROM videos WHERE MATCH(title, description) AGAINST (? IN NATURAL LANGUAGE MODE) LIMIT ?`, q, limit)
	if err != nil {
		return nil, err
	}
	return collectVideos(rows)
}

// SearchCreators 用 name 的 B-tree 索引做前綴搜尋。
func (s *Store) SearchCreators(ctx context.Context, prefix string, limit int) ([]Creator, error) {
	esc := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(prefix)
	rows, err := s.DB.QueryContext(ctx, `SELECT id, name, followers, created_at FROM creators WHERE name LIKE ? LIMIT ?`, esc+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Creator{}
	for rows.Next() {
		var c Creator
		if err := rows.Scan(&c.ID, &c.Name, &c.Followers, &c.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) AllVideoIDs(ctx context.Context) ([]int64, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT id FROM videos`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) CreateVideo(ctx context.Context, v Video) (int64, error) {
	res, err := s.DB.ExecContext(ctx, `INSERT INTO videos (creator_id, title, description, duration_sec) VALUES (?, ?, ?, ?)`,
		v.CreatorID, v.Title, v.Description, v.DurationSec)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// ApplyPlays 批次累加播放數並 upsert 播放紀錄；consumer 一次傳一批進來，一個交易兩個 SQL。
func (s *Store) ApplyPlays(ctx context.Context, counts map[int64]int64, history []PlayEvent) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if len(counts) > 0 {
		var sb strings.Builder
		args := make([]any, 0, len(counts)*3)
		ids := make([]any, 0, len(counts))
		sb.WriteString(`UPDATE videos SET play_count = play_count + CASE id`)
		for id, n := range counts {
			sb.WriteString(` WHEN ? THEN ?`)
			args = append(args, id, n)
			ids = append(ids, id)
		}
		sb.WriteString(` END WHERE id IN (` + placeholders(len(ids)) + `)`)
		if _, err := tx.ExecContext(ctx, sb.String(), append(args, ids...)...); err != nil {
			return err
		}
	}
	if len(history) > 0 {
		var sb strings.Builder
		args := make([]any, 0, len(history)*3)
		sb.WriteString(`INSERT INTO play_history (user_id, video_id, position_sec) VALUES `)
		for i, h := range history {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(`(?,?,?)`)
			args = append(args, h.UserID, h.VideoID, h.PositionSec)
		}
		sb.WriteString(` AS new ON DUPLICATE KEY UPDATE position_sec = new.position_sec, updated_at = NOW()`)
		if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func (s *Store) ListHistory(ctx context.Context, userID int64, limit int) ([]HistoryItem, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT h.video_id, v.title, h.position_sec, h.updated_at
FROM play_history h JOIN videos v ON v.id = h.video_id
WHERE h.user_id = ? ORDER BY h.updated_at DESC LIMIT ?`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []HistoryItem{}
	for rows.Next() {
		var h HistoryItem
		if err := rows.Scan(&h.VideoID, &h.Title, &h.PositionSec, &h.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// AddFavorite 回傳是否真的新增（重複收藏不會重複計數）。
func (s *Store) AddFavorite(ctx context.Context, userID, videoID int64) (bool, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `INSERT IGNORE INTO favorites (user_id, video_id) VALUES (?, ?)`, userID, videoID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n == 1 {
		if _, err := tx.ExecContext(ctx, `UPDATE videos SET fav_count = fav_count + 1 WHERE id = ?`, videoID); err != nil {
			return false, err
		}
	}
	return n == 1, tx.Commit()
}

func (s *Store) RemoveFavorite(ctx context.Context, userID, videoID int64) (bool, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `DELETE FROM favorites WHERE user_id = ? AND video_id = ?`, userID, videoID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n == 1 {
		if _, err := tx.ExecContext(ctx, `UPDATE videos SET fav_count = fav_count - 1 WHERE id = ? AND fav_count > 0`, videoID); err != nil {
			return false, err
		}
	}
	return n == 1, tx.Commit()
}

func (s *Store) ListFavorites(ctx context.Context, userID int64, limit int) ([]Video, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT `+prefixed("v", videoCols)+` FROM favorites f JOIN videos v ON v.id = f.video_id
WHERE f.user_id = ? ORDER BY f.created_at DESC LIMIT ?`, userID, limit)
	if err != nil {
		return nil, err
	}
	return collectVideos(rows)
}

func prefixed(alias, cols string) string {
	parts := strings.Split(cols, ",")
	for i, p := range parts {
		parts[i] = alias + "." + strings.TrimSpace(p)
	}
	return strings.Join(parts, ", ")
}
