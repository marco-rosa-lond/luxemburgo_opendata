create table dbo.DICT_OWNER_TYPE
(
    owner_type_code varchar(10) not null
        primary key,
    FR              nvarchar(255),
    EN              nvarchar(255),
    PT              varchar(20)
)
go

INSERT INTO LUX_OPENDATA.dbo.DICT_OWNER_TYPE (owner_type_code, FR, EN, PT) VALUES (N'NA', N'Non Accessible', N'Undefined', null);
INSERT INTO LUX_OPENDATA.dbo.DICT_OWNER_TYPE (owner_type_code, FR, EN, PT) VALUES (N'PM', N'Personne Morale', N'Corporation', N'Empresa');
INSERT INTO LUX_OPENDATA.dbo.DICT_OWNER_TYPE (owner_type_code, FR, EN, PT) VALUES (N'PP', N'Personne Physique', N'Physical Person', N'Particular');
