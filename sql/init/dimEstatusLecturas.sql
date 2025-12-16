CREATE TABLE dwh.dimEstatusLecturas
(
    Id INT IDENTITY (1, 1) PRIMARY KEY,
    Estatus SMALLINT NOT NULL,
    Descripcion NVARCHAR(15) NOT NULL
);
