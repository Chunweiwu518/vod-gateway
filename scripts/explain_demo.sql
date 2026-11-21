-- 慢查詢加索引前後對照（50 萬支影片、2000 位創作者）。
-- 執行：scripts/explain_demo.sh
USE vod;
SELECT COUNT(*) AS videos FROM videos;

SELECT '=== A. 創作者的影片列表：拿掉 (creator_id, created_at) 索引 ===' AS step;
ALTER TABLE videos DROP INDEX idx_creator_created;
EXPLAIN ANALYZE
SELECT id, creator_id, title, description, duration_sec, play_count, fav_count, created_at
FROM videos WHERE creator_id = 1234 ORDER BY created_at DESC, id DESC LIMIT 20\G

SELECT '=== A. 加回 (creator_id, created_at) 索引 ===' AS step;
ALTER TABLE videos ADD INDEX idx_creator_created (creator_id, created_at);
EXPLAIN ANALYZE
SELECT id, creator_id, title, description, duration_sec, play_count, fav_count, created_at
FROM videos WHERE creator_id = 1234 ORDER BY created_at DESC, id DESC LIMIT 20\G

SELECT '=== B. 搜尋：LIKE 前後模糊（無法用索引）===' AS step;
EXPLAIN ANALYZE
SELECT id, title FROM videos WHERE title LIKE '%露營 vlog 懶人包%' OR description LIKE '%露營 vlog 懶人包%' LIMIT 20\G

SELECT '=== B. 搜尋：FULLTEXT（ngram）===' AS step;
EXPLAIN ANALYZE
SELECT id, title FROM videos WHERE MATCH(title, description) AGAINST ('"露營 vlog 懶人包"' IN BOOLEAN MODE) LIMIT 20\G
