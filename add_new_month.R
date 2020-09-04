# Загрузка необходимых функций ----------------------------------------------------------------
source("scripts/functions.R")

# Установление связи с базой данных -----------------------------------------------------------
con <- odbcDriverConnect("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=./db_ipp.accdb")

# Входные данные ------------------------------------------------------------------------------

path <- "input/data_2020_07.xlsx"
date_cm <- "2020-07-01"
date_pm <- "2020-06-01"
date_py <- "2019-07-01"
excpt <- c("Республика Марий Эл", "Республика Мордовия", "Республика Татарстан", 
           "Удмуртская Республика", "Чувашская Республика", "Кировская область",
           "Нижегородская область", "Пензенская область", "Самарская область",
           "Саратовская область", "Ульяновская область")

# Обновление базы данных ----------------------------------------------------------------------
data_db <- con %>% 
  load_db() %>% 
  select(region, okved, date, base, mom, yoy)
data_py <- data_db %>% 
  grab_from_db(date_py)
data_pm <- data_db %>% 
  grab_from_db(date_pm)
data_cm <- path %>% 
  load_one_file() %>% 
  match_names() %>% 
  change_coltypes() %>% 
  calculate_new_month(data_pm, data_py, date_cm)
data_cm_vvgu <- data_cm %>% 
  dplyr::filter(region %in% excpt) %>% 
  calculate_new_month_vvgu(date_cm) %>% 
  mutate(date = ymd(date_cm), region = "ВВГУ") %>% 
  select(region, okved, date, base, mom, yoy)
data_sa <- rbind(data_db, data_cm, data_cm_vvgu) %>% 
  quick_sa()
save(data_sa, file = "data/data_2020_07.RData")
data_sa <- data_sa %>% 
  coding_data(con)
con %>% 
  sqlQuery(paste("DELETE FROM data_okved"))
sqlSave(channel = con, dat = data_sa, tablename = "data_okved", rownames = F, append = T, safer = T)

# Обновление макетов --------------------------------------------------------------------------
con %>% 
  load_into_files()
close(con)
