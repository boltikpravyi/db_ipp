# Блок функций для обновления макета ----------------------------------------------------------

# Загружаем файл в среду и приводим к нужному виду 
load_one_file <- function(path) {
  path %>% 
    read_excel(sheet = 1) %>% 
    setNames(.[2,]) %>% 
    .[-(1:2),] %>% 
    set_names(c("okved_emiss", colnames(.)[-1])) %>% 
    gather(region_emiss, yoy, colnames(.)[-1]) %>% 
    mutate(yoy = replace_na(yoy, 100))
}

# Заменяем имена ОКВЭДов и регионов
match_names <- function(x) {
  load(file = "data/names.RData")
  x %>% 
    left_join(okved, by = "okved_emiss") %>% 
    drop_na() %>% 
    .[,-1] %>% 
    left_join(region, by = "region_emiss") %>% 
    drop_na() %>% 
    .[,-1] %>% 
    select(region, okved, yoy)
}

# Нужный тип данных
change_coltypes <- function(x) {
  x %>% 
    mutate_at(vars(region, okved), as_factor) %>% 
    mutate(yoy = as.numeric(yoy))
}

# Загружаем данные из db_ipp в среду
load_db <- function(con) {
  con %>% 
    sqlQuery(paste("SELECT * FROM print_all")) %>% 
    as_tibble()
}

# Отбираем из БД нужную нам дату
grab_from_db <- function(x, filt_date) {
  x %>% 
    mutate(date = as_date(date)) %>% 
    dplyr::filter(date == filt_date) %>% 
    select(region, okved, date, base)
}

# Расчет base, mom, yoy для новых данных
calculate_new_month <- function(data_cm, data_pm, data_py, date_cm) {
  left_join(x = data_cm, y = data_py) %>% 
    drop_na() %>% 
    mutate(base_cm = base * yoy / 100) %>% 
    select(region, okved, base_cm, yoy) %>% 
    left_join(data_pm) %>% 
    mutate(mom = base_cm / base * 100) %>% 
    select(region, okved, date, base_cm, mom, yoy) %>% 
    rename(base = base_cm) %>% 
    mutate(date = ymd(date_cm)) %>% 
    mutate_at(vars(region, okved), as_factor)
}

# Расчет base, mom, yoy для ВВГУ
calculate_new_month_vvgu <- function(data_cm, date_cm) {
  load(file = "data/weights_vvgu.RData")
  data_cm %>% 
    left_join(y = weights_vvgu) %>%
    mutate(base_w = base * weight, mom_w = mom * weight, yoy_w = yoy * weight) %>% 
    select(okved, date, base_w, mom_w, yoy_w, weight) %>%
    group_by(okved) %>%
    summarise(base = sum(base_w) / sum(weight),
              mom = sum(mom_w) / sum(weight),
              yoy = sum(yoy_w) / sum(weight))
}

# Проводим сезонное сглаживание рядов (с учетом новой точки)
quick_sa <- function(x) {
  # calculating Base SA
  reg <- as.character(unique(x$region)) # names of regions to smooth
  result <- as_tibble() # a place to store final results
  # i <- 11
  # j <- 28
  for (i in 1:length(reg)) {
    ved <- as.character(unique(x$okved)) # names of okved to smooth
    # display current region
    print(str_c("---------------- ", reg[i], " ----------------", sep = ""))
    for (j in 1:length(ved)) {
      d <- x %>% 
        arrange(region, okved) %>% 
        dplyr::filter(region == reg[i]) %>% 
        dplyr::filter(okved == ved[j]) # filter region and ved
      if (nrow(d) != 0) {
        model <- try(seas(ts(start = min(year(d$date)), frequency = 12, data = d$base))) # smoothing
        if (is(model, "try-error")) { # if there is an error in smoothing
          # next # ignore it
          d <- bind_cols(d, as_tibble(d$base)) # add BaseSA
          d <- d %>% rename(base_sa = value)
          d <- bind_cols(d, as_tibble(c(d$base) / dplyr::lag(c(d$base), n = 1)) * 100) # add mom_sa
          d <- d %>% rename(mom_sa = value)
          d <- d %>% mutate(mom_sa = replace_na(mom_sa, 100))
        }
        else {
          d <- bind_cols(d, as_tibble(model$series$s11)) # add BaseSA
          d <- d %>% rename(base_sa = x)
          d <- bind_cols(d, as_tibble(c(model$series$s11) / dplyr::lag(c(model$series$s11), n = 1) * 100)) # add mom_sa
          d <- d %>% rename(mom_sa = value)
          d <- d %>% mutate(mom_sa = replace_na(mom_sa, 100))
          
        }
      }
      result <- bind_rows(result, d) # merge with final table
      print(ved[j]) # display current ved
    }
  }
  x <- result
}

# Кодируем данные для заливки в базу
coding_data <- function(x, con) {
  reg <- con %>% 
    sqlQuery(paste("SELECT * FROM names_region")) %>% 
    select(region, region_code)
  ved <- con %>% 
    sqlQuery(paste("SELECT * FROM names_okved")) %>% 
    select(okved, okved_code)
  x %>% 
    left_join(reg, by = "region") %>% 
    .[,-1] %>% 
    left_join(ved, by = "okved") %>% 
    .[,-1] %>% 
    select(region_code, okved_code, date, base, mom, yoy, base_sa, mom_sa) %>% 
    mutate(date = as_date(date))
}

# Заливаем данные в базу
load_into_db <- function(x, con) {
  con %>% 
    sqlQuery(paste("DELETE FROM data_okved")) %>% 
    sqlSave(dat = x, tablename = "data_okved", rownames = F, append = T, safer = T)
}

# Обновляем макеты
load_into_files <- function(con) {
  db <- con %>% 
    sqlQuery(paste('SELECT * FROM print_all')) %>% 
    as_tibble() %>% 
    select(region, okved, date, base_sa, mom_sa, yoy)
  reg <- con %>% 
    sqlQuery(paste('SELECT * FROM names_region')) %>% 
    .[,2] %>% 
    fct_unique() %>% 
    as.character()
  
  for (i in 1:length(reg)) {
    d <- db %>% 
      dplyr::filter(region == reg[i]) %>% 
      select(okved, date, base_sa, mom_sa, yoy) %>% 
      mutate(mom_sa = mom_sa - 100, yoy = yoy - 100, date = as_date(date))
    wb <- loadWorkbook(str_c("output/", reg[i], ".xlsm", sep = ""))
    writeData(wb, sheet = "data", d, colNames = T)
    saveWorkbook(wb, str_c("output/", reg[i], ".xlsm", sep = ""), overwrite = T)
    print(reg[i])
  }
}

# Собираем комментарии Отделений ВВГУ в одни файл
grab_comments <- function(con, excpt) {
  reg <- con %>% 
    sqlQuery(query = "SELECT * FROM names_region") %>% 
    .[,-1] %>% 
    as.character() %>% 
    as_tibble() %>% 
    dplyr::filter(value %in% excpt | value == "ВВГУ")
  comm <- as_tibble()
  for (i in 1:11) {
    wb <- str_c("input/", reg[i, 1], ".xlsm") %>% 
      loadWorkbook()
    df <- wb %>% 
      read.xlsx(sheet = 2, skipEmptyRows = F)
    comm <- bind_rows(comm, df) %>% 
      as_tibble()
    wb %>% 
      deleteData(sheet = 2, cols = 8:9, rows = 2:(nrow(df) + 1), gridExpand = T)
    saveWorkbook(wb, str_c("input/", reg[i, 1], ".xlsm"), overwrite = T)
    print(reg[i, 1])
  }
  colnames(comm) <- c("Регион", "Отрасль", "Основные компании", "Вес в ИПП региона", "Вес в ВВГУ", 
                      "Вес в ИПП ВВГУ", "Последняя дата", "Факторы текущей динамики", 
                      "Факторы краткосрочного прогноза")
  wb <- str_c("input/", reg[12, 1], ".xlsm") %>% 
    loadWorkbook()
  df <- wb %>% 
    read.xlsx(sheet = 2, skipEmptyRows = F)
  colnames(df) <- c("Регион", "Отрасль", "Основные компании", "Вес в ИПП региона", "Вес в ВВГУ", 
                    "Вес в ИПП ВВГУ", "Последняя дата", "Факторы текущей динамики", 
                    "Факторы краткосрочного прогноза")
  d <- bind_rows(comm, df) %>% 
    as_tibble()
  wb %>% 
    deleteData(sheet = 2, cols = 1:10000, rows = 1:10000)
  writeData(wb, d, sheet = 2, colNames = T)
  headerStyle <- createStyle(fontSize = 14, fontName = "Arial Narrow", fontColour = rgb(22/255, 54/255, 92/255), halign = "left",
                             valign = "center", border = "bottom", borderColour = rgb(22/255, 54/255, 92/255), borderStyle = "medium",
                             wrapText = T, textDecoration = "bold")
  wb %>% 
    addStyle(sheet = 2, headerStyle, rows = 1, cols = 1:9, gridExpand = T)
  bodyStyle_l <- createStyle(fontSize = 14, fontName = "Arial Narrow", fontColour = rgb(22/255, 54/255, 92/255), halign = "left",
                             valign = "center", border = "bottom", borderColour = rgb(22/255, 54/255, 92/255), borderStyle = "thin",
                             wrapText = T)
  bodyStyle_r_perc <- createStyle(fontSize = 14, fontName = "Arial Narrow", fontColour = rgb(22/255, 54/255, 92/255), halign = "right",
                                  valign = "center", border = "bottom", borderColour = rgb(22/255, 54/255, 92/255), borderStyle = "thin",
                                  wrapText = T, numFmt = "PERCENTAGE")
  bodyStyle_r_date <- createStyle(fontSize = 14, fontName = "Arial Narrow", fontColour = rgb(22/255, 54/255, 92/255), halign = "right",
                                  valign = "center", border = "bottom", borderColour = rgb(22/255, 54/255, 92/255), borderStyle = "thin",
                                  wrapText = T, numFmt = "DATE")
  wb %>% 
    addStyle(sheet = 2, bodyStyle_l, rows = 2:(nrow(d) + 1), cols = 1:3, gridExpand = T) %>% 
    addStyle(sheet = 2, bodyStyle_l, rows = 2:(nrow(d) + 1), cols = 8:9, gridExpand = T) %>% 
    addStyle(sheet = 2, bodyStyle_r_perc, rows = 2:(nrow(d) + 1), cols = 4:6, gridExpand = T) %>% 
    addStyle(sheet = 2, bodyStyle_r_date, rows = 2:(nrow(d) + 1), cols = 7, gridExpand = T) %>% 
    setColWidths(sheet = 2, cols = c(1, 2, 3, 4, 5, 6, 7, 8, 9), widths = c(26.43, 31.86, 40, 10, 10, 10, 13, 60, 60)) %>% 
    setRowHeights(sheet = 2, rows = 1, heights = 66.75) %>% 
    setRowHeights(sheet = 2, rows = c(2:(nrow(d) + 1)), heights = 140)
  saveWorkbook(wb, str_c("input/", reg[12, 1], ".xlsm"), overwrite = T) # save VVGU maket with fresh comments
}


# Блок функций для пересчета ретроспективных данных -------------------------------------------
# Загрузка файла
load_revised_file <- function(path) {
  path %>% 
    read_excel(sheet = 1) %>% 
    gather(key = "date", value = "yoy", colnames(.)[-(1:2)]) %>% 
    mutate(date = as.Date(as.numeric(date), origin = "1900-01-01") - 2)
}

# Заменяем имена ОКВЭДов и регионов
change_names <- function(x) {
  load(file = "data/names.RData")
  x %>% 
    left_join(region, by = "region_emiss") %>% 
    .[,-1] %>% 
    left_join(okved, by = "okved_emiss") %>% 
    .[,-1] %>% 
    mutate(date = as_date(date)) %>% 
    select(region, okved, date, yoy)
}

# Заменяем старые yoy и base на новые
change_data <- function(data_new, db_old) {
  dates <- db_old %>% 
    select(date) %>% 
    unique() %>% 
    mutate(date = as_date(date)) %>% 
    as.data.frame()
  db_new <- as_tibble()
  for (i in 13:nrow(dates)) {
    if (i < 25) {
      x <- data_new %>% 
        dplyr::filter(date == dates[i,1]) %>% 
        select(region, okved, yoy)
      y <- db_old %>% 
        dplyr::filter(date == dates[i-12,1]) %>% 
        select(region, okved, base)
      temp <- left_join(y, x, by = c("region", "okved")) %>% 
        add_column(date = rep(dates[i,1], nrow(.))) %>% 
        mutate(yoy = replace_na(yoy, 100)) %>% 
        mutate(base = base * yoy / 100) %>% 
        select(region, okved, date, base, yoy)
      db_new <- bind_rows(db_new, temp)
      print(dates[i,1])
    }
    else {
      x <- data_new %>% 
        dplyr::filter(date == dates[i,1]) %>% 
        select(region, okved, yoy)
      y <- db_new %>% 
        dplyr::filter(date == dates[i-12,1]) %>% 
        select(region, okved, base)
      temp <- left_join(y, x, by = c("region", "okved")) %>% 
        add_column(date = rep(dates[i,1], nrow(.))) %>% 
        mutate(yoy = replace_na(yoy, 100)) %>% 
        mutate(base = base * yoy / 100) %>% 
        select(region, okved, date, base, yoy)
      db_new <- bind_rows(db_new, temp)
      print(dates[i,1])
    }
  }
  db_new
}

# Рассчитываем новые mom
calculate_mom <- function(db_new, db_old) {
  y <- db_old %>% 
    dplyr::filter(date < "2015-01-01") %>% 
    mutate(date = as_date(date)) %>% 
    select(region, okved, date, base, yoy)
  db_new <- bind_rows(y, db_new) %>% 
    arrange(region, okved, date) %>% 
    group_by(region, okved) %>% 
    mutate(mom = base / dplyr::lag(base, n = 1) * 100) %>% 
    mutate(mom = replace_na(mom, 100)) %>% 
    select(region, okved, date, base, mom, yoy)
}

# Рассчет данных по ВВГУ
calculate_vvgu <- function(db_new, db_old, excpt) {
  load(file = "data/weights_vvgu.RData")
  data_vvgu <- db_new %>% 
    dplyr::filter(region %in% excpt) %>% 
    left_join(y = weights_vvgu) %>% 
    mutate(base_w = base * weight, mom_w = mom * weight, yoy_w = yoy * weight) %>% 
    select(okved, date, base_w, mom_w, yoy_w, weight) %>%
    group_by(okved, date) %>%
    summarise(base = sum(base_w) / sum(weight),
              mom = sum(mom_w) / sum(weight),
              yoy = sum(yoy_w) / sum(weight)) %>% 
    mutate(region = "ВВГУ") %>% 
    select(region, okved, date, base, mom, yoy)
  db_new <- bind_rows(db_new, data_vvgu)
}

# Проводим сезонное сглаживание рядов
smooth_data <- function(db_new) {
  # calculating Base SA
  reg <- as.character(unique(db_new$region)) # names of regions to smooth
  result <- as_tibble() # a place to store final results
  for (i in 1:length(reg)) {
    ved <- as.character(unique(db_new$okved)) # names of okved to smooth
    # display current region
    print(str_c("---------------- ", reg[i], " ----------------", sep = ""))
    for (j in 1:length(ved)) {
      d <- db_new %>% 
        arrange(region, okved) %>% 
        dplyr::filter(region == reg[i]) %>% 
        dplyr::filter(okved == ved[j]) # filter region and ved
      if (nrow(d) != 0) {
        model <- try(seas(ts(start = min(year(d$date)), frequency = 12, data = d$base))) # smoothing
        if (is(model, "try-error")) { # if there is an error in smoothing
          # next # ignore it
          d <- bind_cols(d, as_tibble(d$base)) # add BaseSA
          d <- d %>% rename(base_sa = value)
          d <- bind_cols(d, as_tibble(c(d$base) / dplyr::lag(c(d$base), n = 1)) * 100) # add mom_sa
          d <- d %>% rename(mom_sa = value)
          d <- d %>% mutate(mom_sa = replace_na(mom_sa, 100))
        }
        else {
          d <- bind_cols(d, as_tibble(model$series$s11)) # add BaseSA
          d <- d %>% rename(base_sa = x)
          d <- bind_cols(d, as_tibble(c(model$series$s11) / dplyr::lag(c(model$series$s11), n = 1) * 100)) # add mom_sa
          d <- d %>% rename(mom_sa = value)
          d <- d %>% mutate(mom_sa = replace_na(mom_sa, 100))
        }
      }
      result <- bind_rows(result, d) # merge with final table
      print(ved[j]) # display current ved
    }
  }
  db_new <- result
}

# Кодируем данные для заливки в базу
encode_data <- function(db_new, con) {
  reg <- con %>% 
    sqlQuery(paste("SELECT * FROM names_region")) %>% 
    select(region, region_code)
  ved <- con %>% 
    sqlQuery(paste("SELECT * FROM names_okved")) %>% 
    select(okved, okved_code)
  db_new <- db_new %>% 
    left_join(reg, by = "region") %>% 
    .[,-1] %>% 
    left_join(ved, by = "okved") %>% 
    .[,-1] %>% 
    select(region_code, okved_code, date, base, mom, yoy, base_sa, mom_sa) %>% 
    mutate(date = as_date(date))
}
