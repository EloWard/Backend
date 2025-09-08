-- Peak Rank Correction Migration
-- Updates ONLY peak rank values when current rank is higher than stored peak
-- Preserves all current rank data unchanged

-- Step 1: Update peak ranks where current rank is definitively higher
UPDATE lol_ranks 
SET 
    peak_rank_tier = rank_tier,
    peak_rank_division = rank_division,
    peak_lp = lp
WHERE 
    -- Only update records that have peak data to compare against
    peak_rank_tier IS NOT NULL 
    AND peak_lp IS NOT NULL
    AND (
        -- Case 1: Current tier is higher than peak tier
        CASE rank_tier
            WHEN 'IRON' THEN 0
            WHEN 'BRONZE' THEN 1
            WHEN 'SILVER' THEN 2
            WHEN 'GOLD' THEN 3
            WHEN 'PLATINUM' THEN 4
            WHEN 'EMERALD' THEN 5
            WHEN 'DIAMOND' THEN 6
            WHEN 'MASTER' THEN 7
            WHEN 'GRANDMASTER' THEN 8
            WHEN 'CHALLENGER' THEN 9
            ELSE -1
        END > 
        CASE peak_rank_tier
            WHEN 'IRON' THEN 0
            WHEN 'BRONZE' THEN 1
            WHEN 'SILVER' THEN 2
            WHEN 'GOLD' THEN 3
            WHEN 'PLATINUM' THEN 4
            WHEN 'EMERALD' THEN 5
            WHEN 'DIAMOND' THEN 6
            WHEN 'MASTER' THEN 7
            WHEN 'GRANDMASTER' THEN 8
            WHEN 'CHALLENGER' THEN 9
            ELSE -1
        END
        
        OR
        
        -- Case 2: Same tier, current division is higher (I > II > III > IV)
        (
            rank_tier = peak_rank_tier 
            AND rank_tier NOT IN ('MASTER', 'GRANDMASTER', 'CHALLENGER')
            AND (
                CASE rank_division
                    WHEN 'I' THEN 4
                    WHEN 'II' THEN 3
                    WHEN 'III' THEN 2
                    WHEN 'IV' THEN 1
                    ELSE 1
                END > 
                CASE peak_rank_division
                    WHEN 'I' THEN 4
                    WHEN 'II' THEN 3
                    WHEN 'III' THEN 2
                    WHEN 'IV' THEN 1
                    ELSE 1
                END
            )
        )
        
        OR
        
        -- Case 3: Same tier/division, current LP is higher (Master+ or same division)
        (
            rank_tier = peak_rank_tier
            AND (
                -- Master+ ranks (no divisions) OR same division for lower ranks
                rank_tier IN ('MASTER', 'GRANDMASTER', 'CHALLENGER')
                OR rank_division = peak_rank_division
            )
            AND COALESCE(lp, 0) > COALESCE(peak_lp, 0)
        )
    );

-- Step 2: Report number of records that were updated
SELECT 
    changes() as records_updated,
    'Peak rank records corrected where current rank was higher' as description;

-- Step 3: Initialize peak ranks for records that have no peak data
-- (Set peak = current for users who don't have peak data yet)
UPDATE lol_ranks 
SET 
    peak_rank_tier = rank_tier,
    peak_rank_division = rank_division,
    peak_lp = lp
WHERE 
    (peak_rank_tier IS NULL OR peak_lp IS NULL)
    AND rank_tier IS NOT NULL;

-- Step 4: Report number of records that were initialized
SELECT 
    changes() as records_initialized,
    'Peak rank records initialized for users without existing peak data' as description;

-- Step 5: Verification - Check for any remaining data inconsistencies
-- This should return 0 records after successful migration
SELECT 
    COUNT(*) as remaining_inconsistencies,
    'Should be 0 - any results indicate remaining corruption' as status
FROM lol_ranks
WHERE 
    peak_rank_tier IS NOT NULL 
    AND peak_lp IS NOT NULL
    AND rank_tier IS NOT NULL
    AND lp IS NOT NULL
    AND (
        -- Current tier higher than peak tier
        CASE rank_tier
            WHEN 'IRON' THEN 0
            WHEN 'BRONZE' THEN 1
            WHEN 'SILVER' THEN 2
            WHEN 'GOLD' THEN 3
            WHEN 'PLATINUM' THEN 4
            WHEN 'EMERALD' THEN 5
            WHEN 'DIAMOND' THEN 6
            WHEN 'MASTER' THEN 7
            WHEN 'GRANDMASTER' THEN 8
            WHEN 'CHALLENGER' THEN 9
            ELSE -1
        END > 
        CASE peak_rank_tier
            WHEN 'IRON' THEN 0
            WHEN 'BRONZE' THEN 1
            WHEN 'SILVER' THEN 2
            WHEN 'GOLD' THEN 3
            WHEN 'PLATINUM' THEN 4
            WHEN 'EMERALD' THEN 5
            WHEN 'DIAMOND' THEN 6
            WHEN 'MASTER' THEN 7
            WHEN 'GRANDMASTER' THEN 8
            WHEN 'CHALLENGER' THEN 9
            ELSE -1
        END
        
        OR
        
        -- Same tier but current division higher
        (
            rank_tier = peak_rank_tier 
            AND rank_tier NOT IN ('MASTER', 'GRANDMASTER', 'CHALLENGER')
            AND (
                CASE rank_division
                    WHEN 'I' THEN 4
                    WHEN 'II' THEN 3
                    WHEN 'III' THEN 2
                    WHEN 'IV' THEN 1
                    ELSE 1
                END > 
                CASE peak_rank_division
                    WHEN 'I' THEN 4
                    WHEN 'II' THEN 3
                    WHEN 'III' THEN 2
                    WHEN 'IV' THEN 1
                    ELSE 1
                END
            )
        )
        
        OR
        
        -- Same tier/division but current LP higher
        (
            rank_tier = peak_rank_tier
            AND (
                rank_tier IN ('MASTER', 'GRANDMASTER', 'CHALLENGER')
                OR rank_division = peak_rank_division
            )
            AND COALESCE(lp, 0) > COALESCE(peak_lp, 0)
        )
    );

-- Step 6: Sample the corrected data to verify results
SELECT 
    'Sample of corrected peak ranks:' as info,
    COUNT(*) as total_records_with_peaks
FROM lol_ranks 
WHERE peak_rank_tier IS NOT NULL AND peak_lp IS NOT NULL;

-- Show a few examples of the corrected data
SELECT 
    twitch_username,
    rank_tier,
    rank_division, 
    lp as current_lp,
    peak_rank_tier,
    peak_rank_division,
    peak_lp,
    'Current and peak should be consistent now' as note
FROM lol_ranks 
WHERE peak_rank_tier IS NOT NULL 
    AND peak_lp IS NOT NULL 
    AND rank_tier IS NOT NULL
ORDER BY 
    CASE rank_tier
        WHEN 'CHALLENGER' THEN 9
        WHEN 'GRANDMASTER' THEN 8
        WHEN 'MASTER' THEN 7
        WHEN 'DIAMOND' THEN 6
        WHEN 'EMERALD' THEN 5
        WHEN 'PLATINUM' THEN 4
        WHEN 'GOLD' THEN 3
        WHEN 'SILVER' THEN 2
        WHEN 'BRONZE' THEN 1
        WHEN 'IRON' THEN 0
        ELSE -1
    END DESC,
    CASE rank_division
        WHEN 'I' THEN 4
        WHEN 'II' THEN 3
        WHEN 'III' THEN 2
        WHEN 'IV' THEN 1
        ELSE 0
    END DESC,
    lp DESC
LIMIT 10;
