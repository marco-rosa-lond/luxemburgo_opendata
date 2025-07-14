create table dbo.DICT_INDICATION_OF_USE
(
    induti_code varchar(10) not null
        primary key,
    FR          nvarchar(255),
    EN          nvarchar(255),
    PT          varchar(100)
)
go

INSERT INTO LUX_OPENDATA.dbo.DICT_INDICATION_OF_USE (induti_code, FR, EN, PT) VALUES (N'0', N'Neant', null, null);
INSERT INTO LUX_OPENDATA.dbo.DICT_INDICATION_OF_USE (induti_code, FR, EN, PT) VALUES (N'A', N'Ambulance', N'Ambulance', N'Ambulância');
INSERT INTO LUX_OPENDATA.dbo.DICT_INDICATION_OF_USE (induti_code, FR, EN, PT) VALUES (N'C', N'Location avec chauffeur', N'Rental vehicle with driver', N'Aluguer com condutor');
INSERT INTO LUX_OPENDATA.dbo.DICT_INDICATION_OF_USE (induti_code, FR, EN, PT) VALUES (N'D', N'Depanneuse', N'Tow truck', N'Camião de guincho');
INSERT INTO LUX_OPENDATA.dbo.DICT_INDICATION_OF_USE (induti_code, FR, EN, PT) VALUES (N'F', N'Vehicule d''instruction', N'Driving school vehicle', N'Escola de condução');
INSERT INTO LUX_OPENDATA.dbo.DICT_INDICATION_OF_USE (induti_code, FR, EN, PT) VALUES (N'I', N'Industriel', N'Industrial', N'Industrial');
INSERT INTO LUX_OPENDATA.dbo.DICT_INDICATION_OF_USE (induti_code, FR, EN, PT) VALUES (N'L', N'Location sans chauffeur', N'Rental vehicle', N'Aluguer');
INSERT INTO LUX_OPENDATA.dbo.DICT_INDICATION_OF_USE (induti_code, FR, EN, PT) VALUES (N'M', N'Motor-home', N'Motor-home', N'Autocaravana');
INSERT INTO LUX_OPENDATA.dbo.DICT_INDICATION_OF_USE (induti_code, FR, EN, PT) VALUES (N'P', N'Vehicule d''incendie', N'Fire truck', N'Viatura dos bombeiros');
INSERT INTO LUX_OPENDATA.dbo.DICT_INDICATION_OF_USE (induti_code, FR, EN, PT) VALUES (N'R', N'Ramassage scolaire', N'Vehicle used for School Transportation', N'Transporte Escolar');
INSERT INTO LUX_OPENDATA.dbo.DICT_INDICATION_OF_USE (induti_code, FR, EN, PT) VALUES (N'S', N'Vehicule de secours', N'Emergency vehicle', N'Emergência');
INSERT INTO LUX_OPENDATA.dbo.DICT_INDICATION_OF_USE (induti_code, FR, EN, PT) VALUES (N'T', N'Taxi', N'Taxi', N'Táxi');
INSERT INTO LUX_OPENDATA.dbo.DICT_INDICATION_OF_USE (induti_code, FR, EN, PT) VALUES (N'X', N'Mma >3500 (Catégorie 59)', N'Mma >3500 (category 59)', N'MMA > 3500 (categoria 59)');
INSERT INTO LUX_OPENDATA.dbo.DICT_INDICATION_OF_USE (induti_code, FR, EN, PT) VALUES (N'Z', N'Vehicule forain', N'Vehicle for fair', N'Veículo ambulante');
