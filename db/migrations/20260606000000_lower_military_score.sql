-- migrate:up
-- Lower pre-filter military score from 7 to 5 to match the new default.
-- Only touches aircraft that received exactly the old deterministic score of 7
-- via the military pre-filter (story_tags contains 'military'). Aircraft that
-- were subsequently re-scored higher by the LLM are left untouched.
UPDATE enriched_aircraft
SET story_score = 5
WHERE story_tags @> ARRAY['military']::text[]
  AND story_score = 7;

-- migrate:down
UPDATE enriched_aircraft
SET story_score = 7
WHERE story_tags @> ARRAY['military']::text[]
  AND story_score = 5;
