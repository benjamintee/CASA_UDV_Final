library(UK2GTFS)

path_in <- "large/national_rail_timetable.zip"
path_out <- "large"

dir.create(path_out, showWarnings = FALSE)

# Step 1: Convert CIF → GTFS (use ncores - 1 to leave one for OS)
message("Converting CIF to GTFS — this takes ~10-20 mins...")
gtfs <- atoc2gtfs(
  path_in = path_in,
  ncores = max(1, parallel::detectCores() - 1)
)

# Step 2: Clean and validate
gtfs <- gtfs_clean(gtfs)
gtfs <- gtfs_force_valid(gtfs)

# Step 3: Write output
gtfs_write(gtfs, folder = path_out, name = "national_rail_gtfs")

message("✓ Done → large/national_rail_gtfs.zip")
