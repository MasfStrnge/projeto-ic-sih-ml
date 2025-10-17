#######################################################################
####### EXTRAÇÃO DE DADOS DO INMET (INSTITUTO DE METEREOLOGIA) ########
#######################################################################


# INSTALANDO PACOTES
install.packages("remotes") # pacote para acessar repositorio remoto
install.packages("duckdb")  # pacote do banco de dados DuckDB
install.packages("DBI")     # Interface para banco de dados do R
install.packages("duckplyr") # pacote com função para exploração de dados.
install.packages("BrazilMet") # pacote de API para estações automáticas
install.packages("brazilinmet") #uma api do INMET

remotes::install_github("italocegatta/BrazilMet") # pacote de API para estações automáticas


# CARREGANDO PACOTES
library(BrazilMet)
library(DBI)
library(duckdb)
library(duckplyr)

# Conectando o DuckDB
connection <- dbConnect(duckdb(), dbdir = "IC_database.duckdb", read_only = FALSE)

codigo_estacoes_norte <- c(
  "A108", "A140", "A138", "A137", "A102", "A136", "A104",
  "A113", "A120", "A128", "A110", "A117", "A109", "A112", 
  "A121", "A111", "A119", "A101", "A133", "A122", "A144",
  "A123", "A126", "A125", "A134", "A124", "A251", "A249",
  "A242", "A244", "A253", "A201", "A226", "A228", "A236",
  "A248", "A202", "A241", "A252", "A231", "A240", "A209",
  "A246", "A239", "A235", "A232", "A210", "A212", "A211",
  "A245", "A254", "A214", "A215", "A256", "A233", "A250",
  "A216", "A230", "A227", "A213", "A234", "A229", "A247",
  "A940", "A939", "A925", "A938", "A135", "A053", "A054",
  "A021", "A044", "A043", "A049", "A038", "A039", "A019",
  "A055", "A041", "A040", "A009", "A010", "A020", "A018",
  "A051", "A050", "A048", "A052"
)

# Baixando o dados da região norte :  e carrengando no banco de dados

download_AWS_INMET_daily(stations = codigo_estacoes_norte, 
                         start_date = "2016-01-01",end_date = "2016-06-30") %>%
                         dbWriteTable(con,"INMET_dados",., append = TRUE)


dbListTables(con)
dbGetQuery(con, "SELECT * FROM INMET_dados LIMIT 100")
dbExecute(con, "DROP TABLE INMET_dados")
dbGetQuery(con, "SELECT COUNT(*) FROM INMET_dados")
dbExecute(con, "INSERT INTO INMET_dados SELECT * FROM read_csv_auto('dados_A018]_H_2015-01-01_2024-12-31.csv')")


dbExecute(con, "COPY (SELECT * FROM sih_dados) TO 'dados_sih.csv' (HEADER, DELIMITER ',')")


# finalizando a conexão com o Duckdb
dbDisconnect(con, shutdown = TRUE)
