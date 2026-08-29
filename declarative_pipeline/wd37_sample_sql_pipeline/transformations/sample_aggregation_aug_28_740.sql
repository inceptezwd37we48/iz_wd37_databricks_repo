-- This file defines a sample transformation.
-- Edit the sample below or add new transformations
-- using "+ Add" in the file browser.

CREATE MATERIALIZED VIEW sample_aggregation_aug_28_740 AS
SELECT
    user_type,
    COUNT(user_type) AS total_count
FROM sample_users_aug_28_740
GROUP BY user_type;
