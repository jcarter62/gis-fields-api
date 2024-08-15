select left(isnull(convert(varchar, FLDField_ID),'-'),14) as WMIS_Field_ID
    , left(isnull(convert(varchar(12), FLDBeginDate),'-'),12) as Begin_Date
    , left(isnull(convert(varchar, FLDAcres),'-'),8) as Acres
    , left(FLDLegaldesc, 30) as Legal_Description
    , left(case
             when GISField_ID = 0 then '-'
             else convert(varchar, GISField_ID)
           end,13) as GIS_Field_ID
    , NewWMISFields as Prior_WMIS_Field
    , ErrorMessage as Prior_Legal_Description
 from GISandWMISFields
order by GISField_ID, FLDField_ID
