# Bayesian Hierarchical Negative Binomial Model of US Public Event Attendance
# Social Statistics C116

# Outcome: attendance (count of people)
# Question: What structural factors predict attendance at US public events, and what do posterior estimates reveal about the social vibrancy of US localities?
# Model: Hierarchical Negative Binomial with random intercepts for event category and US state

# ---- 0. Libraries ----
library(tidyverse)
library(lubridate)
library(rstanarm)     
library(bayesplot)     # diagnostics
library(loo)           # model comparison
library(projpred)      # variable selection
library(jsonlite)      # parse event_labels JSON
library(scales)
library(ggrepel)       
 
options(mc.cores = parallel::detectCores())   
set.seed(42)
theme_set(theme_minimal(base_size = 12))


# --- 1. Load + Feature Engineering ---
df_raw <- read.csv("/Users/felipe/Downloads/1. Projects/Data Eng Personal Project/analysis/us_events_snapshot.csv")

glimpse(df_raw)
cat("Rows:", nrow(df_raw), "\n")

northeast <- c("Connecticut","Maine","Massachusetts","New Hampshire", "Rhode Island","Vermont","New Jersey","New York", "Pennsylvania")
midwest   <- c("Illinois","Indiana","Michigan","Ohio","Wisconsin", "Iowa","Kansas","Minnesota","Missouri","Nebraska", "North Dakota","South Dakota")
south     <- c("Delaware","Florida","Georgia","Maryland", "North Carolina","South Carolina","Virginia", "West Virginia","Alabama","Kentucky","Mississippi",
"Tennessee","Arkansas","Louisiana","Oklahoma","Texas",
               "District of Columbia")

df <- df_raw %>% mutate(
  start_dt    = ymd_hms(start_local), 
  end_dt      = ymd_hms(end_local),
  day_of_week = wday(start_dt, label = TRUE, week_start = 1), 
  is_weekend  = day_of_week %in% c("Sat", "Sun"),
  start_hour  = hour(start_dt),
  month       = month(start_dt, label = TRUE),
  
  duration_hours = duration / 3600,
  region = case_when(
      state %in% northeast ~ "Northeast",
      state %in% midwest   ~ "Midwest",
      state %in% south     ~ "South",
      TRUE                 ~ "West"
    )
  )


parse_labels <- function(x) {
  if (is.na(x) || x == "" || x == "[]") {
    return(tibble(n = 0L, primary = NA_character_))
  }
  parsed <- fromJSON(x)
  if (!is.data.frame(parsed) || nrow(parsed) == 0) {
    return(tibble(n = 0L, primary = NA_character_))
  }
  tibble(n = nrow(parsed), primary = parsed$label[which.max(parsed$weight)])
}

label_info <- map_dfr(df$event_labels, parse_labels)
df <- df %>%
  mutate(
    label_count   = label_info$n,
    primary_label = label_info$primary
  )

# --- 2. Missingness Analysis ---

miss_tbl <- df %>% summarise(
  n_total = n(),
  n_missing_att = sum(is.na(attendance)),
  pct_missing = round(mean(is.na(attendance)) * 100, 1)
)
print(miss_tbl)

miss_compare <- df %>% 
  mutate(has_attendance = !is.na(attendance)) %>%
  group_by(has_attendance) %>%
  summarise(
    n = n(),
    mean_local = mean(local_rank, na.rm = TRUE),
    mean_natl = mean(national_rank, na.rm = TRUE),
    .groups = "drop")
print(miss_compare)



# --- 3. Modeling Sample ---
state_abbr_map <- c(
  AK="Alaska", AL="Alabama", AR="Arkansas", AZ="Arizona", CA="California",
  CO="Colorado", CT="Connecticut", DE="Delaware", FL="Florida", GA="Georgia",
  HI="Hawaii", IA="Iowa", ID="Idaho", IL="Illinois", IN="Indiana", KS="Kansas",
  KY="Kentucky", LA="Louisiana", MA="Massachusetts", MD="Maryland", ME="Maine",
  MI="Michigan", MN="Minnesota", MO="Missouri", MS="Mississippi", MT="Montana",
  NC="North Carolina", ND="North Dakota", NE="Nebraska", NH="New Hampshire",
  NJ="New Jersey", NM="New Mexico", NV="Nevada", NY="New York", OH="Ohio",
  OK="Oklahoma", OR="Oregon", PA="Pennsylvania", RI="Rhode Island",
  SC="South Carolina", SD="South Dakota", TN="Tennessee", TX="Texas",
  UT="Utah", VA="Virginia", VT="Vermont", WA="Washington", WI="Wisconsin",
  WV="West Virginia", WY="Wyoming", "D.C."="District of Columbia"
)

events <- df %>%
  filter(!is.na(attendance), attendance > 0,
         !is.na(primary_label), primary_label != "",
         !is.na(category),
         !is.na(state), state != "",
         !is.na(duration_hours)) %>%
  mutate(
    state = unname(if_else(state %in% names(state_abbr_map), state_abbr_map[state], state)),
    local_lift    = local_rank - national_rank,
    category      = factor(category),
    state         = factor(state),
    region        = factor(region),
    primary_label = factor(primary_label),
    day_of_week   = factor(day_of_week),
    log_duration  = as.numeric(scale(log1p(duration_hours))),
    start_hour_z  = as.numeric(scale(start_hour)),
    label_count_z = as.numeric(scale(label_count))
  ) %>%
  as.data.frame() %>%
  mutate(category = droplevels(category),
         state    = droplevels(state))

cat("Modeling sample:", nrow(events), "events\n")
cat("state levels:", nlevels(events$state), "\n")



# --- 4. Visual Summaries ---

ggsave("fig1_attendance_dist.png",
  ggplot(events, aes(attendance)) +
    geom_histogram(bins = 60, fill = "#2E75B6", alpha = .85) +
    scale_x_log10(labels = comma) +
    labs(title = "Distribution of Event Attendance (log scale)",
         x = "Attendance", y = "Count of events"),
  width = 7, height = 4, dpi = 150)
 
ggsave("fig2_attendance_category.png",
  ggplot(events, aes(reorder(category, attendance, median), attendance)) +
    geom_boxplot(fill = "#9FE1CB", outlier.size = .3) +
    scale_y_log10(labels = comma) + coord_flip() +
    labs(title = "Attendance by Event Category", x = NULL, y = "Attendance (log scale)"),
  width = 7, height = 4.5, dpi = 150)
 
ggsave("fig3_attendance_region.png",
  ggplot(events, aes(region, attendance, fill = region)) +
    geom_boxplot(outlier.size = .3, show.legend = FALSE) +
    scale_y_log10(labels = comma) +
    labs(title = "Attendance by US Region", x = NULL, y = "Attendance (log scale)"),
  width = 7, height = 4, dpi = 150)
 
ggsave("fig4_attendance_dow.png",
  ggplot(events, aes(day_of_week, attendance)) +
    geom_boxplot(fill = "#FAC775", outlier.size = .3) +
    scale_y_log10(labels = comma) +
    labs(title = "Attendance by Day of Week", x = NULL, y = "Attendance (log scale)"),
  width = 7, height = 4, dpi = 150)



# --- 5. Numerical Summaries ---

summary_by_cat <- events %>% 
  group_by(category) %>%
  summarise(
    n = n(),
    mean_att = round(mean(attendance)),
    median_att = median(attendance),
    sd_att = round(sd(attendance)),
    .groups = "drop"
  ) %>% 
  arrange(desc(median_att))
print(summary_by_cat)

# Variance >> mean confirms overdispersion -> Negative Binomial, not Poisson
od <- events %>%
  summarise(mean_att = mean(attendance), 
            var_att = var(attendance),
            ratio = var(attendance) / mean(attendance)
            )
print(od)