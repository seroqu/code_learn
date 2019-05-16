PRINT('BEGIN: Recreate Entity Values ');
BEGIN 
 
-- SCRIPT TO ENTITY VALUES 
DECLARE 
@Id									  INT,
@guidAttrib						      UNIQUEIDENTIFIER, 
@entSrc								  NVARCHAR(100),                  
@attribsrc							  NVARCHAR(100),
@attribAttributeType				  AS INT,
@guidentity							  UNIQUEIDENTIFIER,
@guidParameter						  UNIQUEIDENTIFIER, 
@value							      NVARCHAR(max),       
@query								  AS NVARCHAR(500),             
@idEntRelated						  INT,         
@queryEntRelated					  AS NVARCHAR(200), 
@entSrcRelated						  AS NVARCHAR(200), 
@entSrcRelatedForGuid				  AS NVARCHAR(200),  
@entityValueSection					  AS NVARCHAR(max),
@contentFinal					      AS NVARCHAR(max),
@idEntityValue						  AS NVARCHAR(200),
@finalEntity						  AS NVARCHAR(200) , 
@objcursor							  AS CURSOR ,
@vsql								  AS NVARCHAR(max),
@vquery								  AS NVARCHAR(max),
@idDynamic							  AS INT,
@disable							  AS NVARCHAR(10),
@idFinalEntInt					      AS INT,
@deployOnParent					      AS NVARCHAR(1),
@contentType						  NVARCHAR(1),
@guidParameterVarchar                 AS NVARCHAR(36), 
@guidentityVarchar                    AS NVARCHAR(36),
@entSurrogateKey                      AS NVARCHAR(200),
@entSurrogateKeyParametricEntity      AS NVARCHAR(200),
@guidMDForSystemEntity			      NVARCHAR(36),
@entType							  AS INT,
@guidColumnName                       AS NVARCHAR(200),
@tmp2                                 AS NVARCHAR(max),
@guidRootObject						  AS UNIQUEIDENTIFIER,
@Input                                AS VARBINARY(MAX)

-- drop temporary table if exists for columns
IF OBJECT_ID('tbBA_Tmp_SQL_INSERT') IS NOT NULL 
	DROP TABLE tbBA_Tmp_SQL_INSERT
  
CREATE TABLE tbBA_Tmp_SQL_INSERT ( 
	guidObject uniqueidentifier NULL,
	guidObjectParent uniqueidentifier NULL,
	objName NVARCHAR(256) NOT NULL,
	objContent varbinary(max) NULL,
	objType int NOT NULL,
	objTypeName NVARCHAR(50) NULL,
	deployOnParent bit NOT NULL,
	modifiedDate datetime NULL,
	modifiedByUser NVARCHAR(100) NULL,
	mtdVersion int NULL,
	rootObject uniqueidentifier NULL,
	changeSetId int NULL,
	objContentResolved varbinary(max) NULL,
	deleted bit NULL,
	contentFormat tinyint NULL,
	tmp NVARCHAR(max)
)

	SELECT @guidRootObject = guidObject FROM BABIZAGICATALOG WHERE objType=121
-- CURSOR TO READ EACH PARAMETER ENTITY WITH ENT TYPE = 2
	DECLARE cParameterEntity  CURSOR FORWARD_ONLY STATIC FOR
	SELECT   idEnt,entSrc ,guidEnt,entContentType,entSurrogateKey,entType FROM Entity  WHERE  (entType =2  and entSrcType = 1)   and ident not in (12,2)   and ident not in (12,2)
	OPEN    cParameterEntity
	FETCH   cParameterEntity INTO   @id,@entSrc,@guidentity,@contentType,@entSurrogateKeyParametricEntity,@entType
	WHILE   (@@FETCH_STATUS = 0 )
	BEGIN
	SET     @entityValueSection  = ''
	-- TABLE EXIST ?
	IF OBJECT_ID(@entSrc) IS NOT NULL  
	BEGIN	

		SET @guidColumnName = @entSrc

		DECLARE attribs CURSOR FORWARD_ONLY STATIC FOR 
					SELECT  guidAttrib, attribSrc, attribAttributeType, idEntRelated, re.entSrc, re.entSurrogateKey 
					FROM Attrib a 
					INNER JOIN entity e ON a.idEnt = e.idEnt 
					LEFT JOIN entity re ON a.idEntRelated = re.idEnt AND re.entType IN (2,3) AND re.idEnt > 1  AND re.entSrcType = 1
					WHERE e.idEnt = @Id AND (a.idEntRelated <> 1 OR a.idEntRelated is null) 
		
		-- SPECIAL VALIDATION FOR SYSTEM ENTITIES
		IF (@entSrc ='ORGPOSITION')
			SET @guidColumnName='Position'

		IF (@entSrc ='ORG')
			SET @guidColumnName='Organization'




		-- DYNAMIC CURSOR FOR EVERY TABLE 
		SET @vquery = ' select '+@entSurrogateKeyParametricEntity+',guid'+@guidColumnName+',finalEnt from  '+@entSrc+' order by 1 asc '
		SET @vsql = 'set @cursor = cursor forward_only static for ' + @vquery + ' open @cursor;'
		EXEC SYS.SP_EXECUTESQL @vsql ,N'@cursor cursor output' ,@objcursor OUTPUT

		FETCH NEXT FROM @objcursor INTO @idDynamic,@guidParameter,@idFinalEntInt
		WHILE (@@FETCH_STATUS = 0)
		BEGIN
			-- VALIDATE SYSTEM  VALUES, PROCESS ONLY ENTITY VALUES THAT ARE NOT IN THE INITIAL MD
			SET @guidMDForSystemEntity='process'
									
			IF @entType = 3
			BEGIN
				SET @guidMDForSystemEntity = (select guidObject from BABIZAGICATALOG where guidObject= 	@guidParameter and  objType = 488 );
			END 
	
			IF(@guidMDForSystemEntity IS NOT NULL AND @guidMDForSystemEntity !='')
			BEGIN
						
						
				OPEN    attribs	     	
				--fetch first from attribs;
				FETCH attribs INTO @guidAttrib, @attribsrc, @attribAttributeType, @idEntRelated, @entSrcRelated, @entSurrogateKey
				WHILE   (@@FETCH_STATUS = 0 )
				BEGIN		
					SET @value =''
					-- SELECT TO ENTITY PARAMETER AND GET VALUE TO SET 
					if(@attribAttributeType < 100  )
					begin
						 IF (ISNUMERIC(@value)=0 OR @attribAttributeType not IN (12))
						BEGIN
						
							SET @query=  N'    (select  @value = CONVERT(NVARCHAR(max), '+@attribsrc+'  ) from '+@entSrc+' where '+@attribsrc+'  is not null and  guid'+@guidColumnName+' = '''+CONVERT(NVARCHAR(36),@guidParameter)+'''); ';															
							EXEC SP_EXECUTESQL @query,  N'@value NVARCHAR(max) OUTPUT',   @value = @value OUTPUT
						
						end

					 IF (ISNUMERIC(@value)=0 OR @attribAttributeType IN (12))
						BEGIN
		
							SET @query=  N'    (select  @value = CONVERT(NVARCHAR(max), '+@attribsrc+' ,121 ) from '+@entSrc+' where '+@attribsrc+'  is not null and  guid'+@guidColumnName+' = '''+CONVERT(NVARCHAR(36),@guidParameter)+'''); ';															
							EXEC SP_EXECUTESQL @query,  N'@value NVARCHAR(max) OUTPUT',   @value = @value OUTPUT
						end 
					end

					if(@attribAttributeType > 100 and @attribAttributeType < 1000)
					begin

				

						SET @query=  N'    (select  @Input =  CONVERT(varbinary(MAX), '+@attribsrc+')   from  '+@entSrc+' where '+@attribsrc+'  is not null and  guid'+@guidColumnName+' = '''+CONVERT(NVARCHAR(36),@guidParameter)+'''); ';															
						EXEC SP_EXECUTESQL @query,  N'@Input VARBINARY(MAX) OUTPUT',   @Input = @Input OUTPUT

						SET @value=  CAST(N'' as xml).value('xs:base64Binary(sql:variable("@Input"))', 'NVARCHAR(max)')

				

					end


				IF  ((@value IS NOT NULL AND  @value != '') OR @guidAttrib = 'C68F3308-CDCB-43AF-BFB0-F81208D0FEFF'  )
					BEGIN
				
						-- IF @VALUE IS NOT NUMERIC APPLY --> "
						IF (ISNUMERIC(@value)=0 OR @attribAttributeType IN (12,13,14,15,16,21,22,23))
						BEGIN
							SET @value = REPLACE(@value, CHAR(9),'')
							SET @value = REPLACE(@value, CHAR(13),'')
							SET @value = REPLACE(@value, CHAR(10),'\n')
							SET @value = REPLACE(@value, '\','\\')	
							SET @value = REPLACE(@value, '"','\"')	
							SET @value = REPLACE(@value, '\\n','\n')	
							SET @value = '"'+ @value+'"'															
						END
				

						-- IF @VALUE IS BOOLEAN/BIT COVERT
						IF (@attribAttributeType=5)			
							IF (@value ='1')
								SET @value = 'true'
							ELSE
								SET @value = 'false'
																		

						IF (@entSrcRelated is not null)
						BEGIN

							-- SPECIFIC VALIDATIONS FOR SPECIAL SYSTEM ENTITIES 
							set  @entSrcRelatedForGuid=@entSrcRelated
							IF (@entSrcRelated ='ORGPOSITION')
								SET @entSrcRelatedForGuid='Position'
							IF (@entSrcRelatedForGuid ='ORG')
								SET @entSrcRelatedForGuid='Organization'

							SET @queryEntRelated=  N'    (select  @value = CONVERT(NVARCHAR(100), guid'+@entSrcRelatedForGuid +') from '+@entSrcRelated+' where '+@entSurrogateKey+' = '+@value+'); ';															
							EXEC sp_executesql @queryEntRelated,  N'@value NVARCHAR(50) OUTPUT',   @value = @value OUTPUT
							SET @value = '"'+@value+'"'

						END
																		
						-- VALIDATION TO HOLLYDAYSCHEMA
						IF (CONVERT(NVARCHAR(36),@guidAttrib) ='D3579352-8131-4C2B-9816-CEBEAE3B789F')
						BEGIN
							SET @value = (select guidHolidaySchema from HOLIDAYSCHEMA where idHolidaySchema= @value  )
							SET @value = '"'+@value+'"'
						END


						 --VALIDATION TO LGLANGUAGE COUNTRY DOES NOT ALLOW NULL 
						 IF ((CONVERT(NVARCHAR(36),@guidAttrib) ='C68F3308-CDCB-43AF-BFB0-F81208D0FEFF') AND @value is null)
						 BEGIN 
						 SET @value = '"'+@value+'"'

						 END 



						-- JOIN ALL FIELDS IN ONE VAR 
						IF (@entityValueSection != '')
							SET @entityValueSection = @entityValueSection + ',';
						SET @entityValueSection = @entityValueSection + '"' + CONVERT(NVARCHAR(36), @guidAttrib) + '":' + @value;
																
						-- CLEAN VALUE TO NEXT ITERATION
						SET @value = ''
															
					END
				-- END LOOPS FOR EACH ATTRIB GUID:VALUE
				FETCH attribs INTO @guidAttrib, @attribsrc, @attribAttributeType, @idEntRelated, @entSrcRelated, @entSurrogateKey
				END
				CLOSE attribs
				
				-- VALIDATION FOR DATA TYPES 
				SET @contentFinal = ''

				-- Disable attribute
				SET @disable = 'false';					
				IF (@entType = 2)
				BEGIN
					SET @query=  N'    (select  @disable = CONVERT(NVARCHAR(10),  dsbl'+@entSrc+'   ) from '+@entSrc+' where   guid'+@entSrc+' = '''+CONVERT(NVARCHAR(36),@guidParameter)+'''); ';																	
					EXEC SP_EXECUTESQL @query,  N'@disable NVARCHAR(10) OUTPUT',   @disable = @disable OUTPUT;
					IF (@disable ='1')
						SET @disable = 'true';
				END

				SET @contentFinal = '{"idEntityValue":'+ CONVERT(NVARCHAR(80),@idDynamic) +',"finalEntity":"'+CONVERT(NVARCHAR(80),@guidentity)+'","disable":'+@disable+',"fields":{' + @entityValueSection + '}}'
				

				SET @entityValueSection = ''

				-- VALIDATION FOR DEPLOY ON PARENT 
				SET @deployOnParent = 0;
				IF(@Id <= 12 OR @contentType = 1)
					SET @deployOnParent = 1;
												
				-- PREPARE GUIDS TO CREATE INSERT 				
				SET @guidParameterVarchar  =CONVERT(NVARCHAR(36),@guidParameter)
				SET @guidentityVarchar   =  CONVERT(NVARCHAR(36),@guidentity)				
										
				-- ENTITY VALUES INSERTS 
				INSERT INTO tbBA_Tmp_SQL_INSERT  (guidObject,guidObjectParent,objName,objType,objTypeName,deployOnParent,modifiedByUser,mtdVersion,rootObject,changeSetId,objContentResolved,deleted,contentFormat,objContent) 
				VALUES (@guidParameterVarchar,@guidentityVarchar,'entityValues'+@guidParameterVarchar,488,'EntityValue',@deployOnParent,'Upgrader',0,@guidRootObject,1,null,null,0,dbo.fnBA_DB_ClobToBlob(@contentFinal))
			END 

		-- END LOOP FOR DYNAMIC VALUES FROM EACH PARAMETRIC AND SYSTEM VALUE
		FETCH NEXT FROM @objcursor INTO @idDynamic,@guidParameter,@idFinalEntInt
		END
		CLOSE @objcursor
		DEALLOCATE @objcursor
		DEALLOCATE attribs

	END


-- END LOOP FOR PARAMETER ENTITIES
	FETCH cParameterEntity INTO     @id,@entSrc,@guidentity,@contentType,@entSurrogateKeyParametricEntity,@entType
	END
	CLOSE cParameterEntity
	DEALLOCATE cParameterEntity

	delete from BABIZAGICATALOG where objtype = 488 and guidObjectParent != 'D20F5E67-2E44-4FC7-B248-BF8D6DAA07FC' and guidObjectParent in 
	(select guidEnt from entity WHERE  (entType =2  and entSrcType = 1)   and ident not in (12,2))
	
	EXEC('INSERT INTO BABIZAGICATALOG (guidObject,guidObjectParent,objName,objType,objTypeName,deployOnParent,modifiedByUser,mtdVersion,rootObject,changeSetId,objContentResolved,deleted,contentFormat,objContent)
SELECT guidObject,guidObjectParent,objName,objType,objTypeName,deployOnParent,modifiedByUser,mtdVersion,rootObject,changeSetId,objContentResolved,deleted,contentFormat,objContent FROM tbBA_Tmp_SQL_INSERT')

	DROP TABLE tbBA_Tmp_SQL_INSERT
END
PRINT('END:  Recreate Entity Values ')