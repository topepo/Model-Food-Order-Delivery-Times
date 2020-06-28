library(tidyverse)
library(lubridate)
library(fs)

## -----------------------------------------------------------------------------

# Data are from https://github.com/gdhruv80/Model-Food-Order-Delivery-Times
# Those documents do not indicate the original origin and, although there is 
# reference to a data description file, none were included in any commits. 
# Searches for column names did not yield any indication of the data origin. 
# The original repo contained no license information. 

# We'll create a modified version of these data that includes an artificially
# right-censored values that emulate stopping all data collection at 8pm. Any
# data whose order creation is at or after 8pm are discarded and any orders
# whose delivery time is after 8pm are censored at 8pm.

## -----------------------------------------------------------------------------

raw_data <- read_csv("historical_data.csv")

## -----------------------------------------------------------------------------

# Remove some small groups 
small_groups <- 
  raw_data %>% 
  select(store_primary_category) %>% 
  na.omit() %>% 
  group_by(store_primary_category) %>% 
  count() %>% 
  filter(n < 100) %>% 
  select(-n)


filtered_data <- 
  anti_join(raw_data, small_groups, by = "store_primary_category")

## -----------------------------------------------------------------------------

udpated_data <- 
  filtered_data %>% 
  mutate(
    # recode some qualitative columns to be smaller and more descriptive
    market = ifelse(is.na(market_id), "unknown_market", paste0("market_", market_id)),
    market = factor(market),
    
    food_type = ifelse(is.na(store_primary_category),
                       "unknown_type",
                       gsub("-", "_", store_primary_category)), 
    food_type = factor(food_type),
    
    store_id = factor(store_id),
    store_id = as.numeric(store_id),
    store_id = format(store_id),
    store_id = gsub(" ", "0", store_id),
    store_id = factor(paste0("store_", store_id)),

    # first pass at raw delivery time in minutes
    min_to_delivery = difftime(actual_delivery_time, created_at, units = "mins"),
    min_to_delivery = as.numeric(min_to_delivery),
  ) %>% 
  # Remove super-fast or super-long orders and potentially nonsensical data
  filter(min_to_delivery > 0 & min_to_delivery <= 120 & 
           total_outstanding_orders >= 0 & min_item_price >= 0 & 
           total_onshift_dashers >= 0 & total_busy_dashers >= 0 & 
           subtotal > 0) %>% 
  as_tibble()

## -----------------------------------------------------------------------------

delivery_times <- 
  udpated_data %>% 
  # sample_n(10000) %>% 
  mutate(
    # Induce right censoring by stopping all data collection at 8pm
    censor_time = ymd_hms(paste(as.character(date(actual_delivery_time)), "20:00:00")),
    obs_time = case_when(
      actual_delivery_time >= censor_time ~ censor_time,
      TRUE ~ actual_delivery_time),
    censored = obs_time == censor_time,
    min_to_delivery = difftime(obs_time, created_at, units = "mins"),
    min_to_delivery = as.numeric(min_to_delivery)
  ) %>% 
  # Remove data that would not have been collected
  filter(hour(created_at) < 20) %>% 
  select(created_at, actual_delivery_time, total_items, num_distinct_items,
         subtotal, min_item_price, max_item_price, total_outstanding_orders,
         market, food_type, min_to_delivery, censored, contains("dashers")) %>% 
  na.omit()

## -----------------------------------------------------------------------------

saveRDS(delivery_times, file = "~/tmp/delivery_times.rda", compress = "xz", version = 2)

fs::file_info("~/tmp/delivery_times.rda")

