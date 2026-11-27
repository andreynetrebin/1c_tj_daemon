-- Add the 1C technological journal Regions property to an existing v2 table.
ALTER TABLE NetrebinAA.tj_events_ut_v2
    ADD COLUMN IF NOT EXISTS regions String AFTER locks;
