create table dbo.DICT_OPE
(
    operation_code varchar(10) not null
        primary key,
    FR             varchar(50),
    EN             varchar(50),
    PT             varchar(50)
)
go

INSERT INTO LUX_OPENDATA.dbo.DICT_OPE (operation_code, FR, EN, PT) VALUES (N'E', N'Exportation (immatriculé =< 1 ans)', N'Export (registered = 1 year)', N'Exportação (matricula = 1 ano)');
INSERT INTO LUX_OPENDATA.dbo.DICT_OPE (operation_code, FR, EN, PT) VALUES (N'E1', N'Exportation', N'Export', N'Exportação');
INSERT INTO LUX_OPENDATA.dbo.DICT_OPE (operation_code, FR, EN, PT) VALUES (N'H', N'Hors circulation', N'Vehicle registration suspension', N'Cancelamento');
INSERT INTO LUX_OPENDATA.dbo.DICT_OPE (operation_code, FR, EN, PT) VALUES (N'I', N'Importation', N'Import', N'Importação');
INSERT INTO LUX_OPENDATA.dbo.DICT_OPE (operation_code, FR, EN, PT) VALUES (N'N', N'Nouvelle Immatriculation', N'New registration', N'Novo Registo');
INSERT INTO LUX_OPENDATA.dbo.DICT_OPE (operation_code, FR, EN, PT) VALUES (N'T', N'Transcription', N'Transfer of registered keeper', N'Transferência de Proprietário');
