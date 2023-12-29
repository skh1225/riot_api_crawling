--최근 14일간 데이터 match_summary.recent_solo 뷰로 생성
SELECT * FROM match_summary.solo WHERE date>=DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 14 DAY) and p_gameEndedInEarlySurrender=false;


--daily pick, win, total
SELECT 
date, 
tier,
p_championId,
p_teamPosition,
COUNT(1) as pick,
COUNT(NULLIF(p_win,false)) as win,
(SUM(COUNT(1)) OVER(PARTITION BY date, tier))/10 as total
 FROM match_summary.recent_solo GROUP BY date, tier, p_championId, p_teamPosition;

--daily ban
SELECT 
date, 
tier,
ban as championId,
COUNT(1) as bans
FROM match_summary.recent_solo
GROUP BY date,tier,ban
