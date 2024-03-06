using Log;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sap.Data.Hana;

namespace Model.Query
{
    public class DbQuery : IDbQuery
    {
        private SqlConnection connection = null;
        private readonly string connectionString;
        private readonly string SCHEMA;
        private readonly IEventLogStore CsvGeneratorLog;

        public DbQuery(IEventLogStore csvGeneratorLog)
        {
            CsvGeneratorLog = csvGeneratorLog;
            this.connectionString = ConfigurationManager.ConnectionStrings["SAPSQlContext"].ConnectionString;
            this.SCHEMA = ConfigurationSettings.AppSettings.Get("schema").ToString();
        }

        /// <summary>
        /// Funcion para traer el listado de los documentos que se van a procesar, el orden de los campos puede varias pero no se puede cambiar el nombre ni disminuir la cantidad
        /// </summary>
        /// <returns> Devuelve una datatable. Columnas son los campos del query y cada fila es un documento </returns>
        public DataTable GetDocumentsData()
        {
            try
            {
                /*string queryString = "select \"DOCNUM\", \"idnumeracion\", \"numero\", \"idambiente\", \"fechadocumento\", \"fechavencimiento\", \"tipofactura\", \"tipooperacion\", \"notas\", \"fechaimpuestos\", \"moneda\", \"cufe\", \"idreporte\", \"correocopia\", \"FP_idmetodopago\", " +
                                    "\"FP_idmediopago\", \"FP_fechavencimiento\", \"FP_identificador\", \"FP_dias\", \"PF_fechainicial\", \"PF_fechafinal\", \"idconceptonota\", \"REF_idnumeracion\", \"REF_numero\", \"ENT_fecha\", \"ENT_idciudad\",  " +
                                    "\"ENT_direccion\", \"ENT_codigopostal\", \"ENT_TRANS_idtipopersona\", \"ENT_TRANS_idactividadeconomica\", \"ENT_TRANS_nombrecomercial\", \"ENT_TRANS_idciudad\", \"ENT_TRANS_direccion\",  " +
                                    "\"ENT_TRANS_codigopostal\", \"ENT_TRANS_nombres\", \"ENT_TRANS_apellidos\", \"ENT_TRANS_idtipodocumentoidentidad\", \"ENT_TRANS_identificacion\", \"ENT_TRANS_digitoverificacion\",  " +
                                    "\"ENT_TRANS_idtiporegimen\", \"ENT_TRANSDF_Idciudad\", \"ENT_TRANSDF_direccion\", \"ENT_TRANSDF_codigopostal\", \"ENT_TRANS_matriculamercantil\", \"ENT_TRANS_emailcontacto\", \"ENT_TRANS_emailentrega\",  " +
                                    "\"ENT_TRANS_telefono\", \"ENT_TRANS_obligaciones\", \"ENT_metododepago\", \"ENT_condicionesdeentrega\", \"FACT_idtipopersona\", \"FACT_idactividadeconomica\", \"FACT_nombrecomercial\", \"FACT_idciudad\",  " +
                                    "\"FACT_direccion\", \"FACT_codigopostal\", \"FACT_nombres\", \"FACT_apellidos\", \"FACT_idtipodocumentoidentidad\", \"FACT_identificacion\", \"FACT_digitoverificacion\", \"FACT_idtiporegimen\",  " +
                                    "\"FACT_DIR_idciudad\", \"FACT_DIR_direccion\", \"FACT_DIR_codigopostal\", \"FACT_matriculamercantil\", \"FACT_emailcontacto\", \"FACT_emailentrega\", \"FACT_telefono\", \"FACT_obligaciones\",  " +
                                    "\"ADQUIR_idtipopersona\", \"ADQUIR_idactividadeconomica\", \"ADQUIR_nombrecomercial\", \"ADQUIR_idciudad\", \"ADQUIR_direccion\", \"ADQUIR_codigopostal\", \"ADQUIR_nombres\", \"ADQUIR_apellidos\",  " +
                                    "\"ADQUIR_idtipodocumentoidentidad\", \"ADQUIR_identificacion\", \"ADQUIR_digitoverificacion\", \"ADQUIR_idtiporegimen\", \"ADQUIR_DIR_idciudad\", \"ADQUIR_DIR_direccion\", \"ADQUIR_DIR_codigopostal\",  " +
                                    "\"ADQUIR_matriculamercantil\", \"ADQUIR_emailcontacto\", \"ADQUIR_emailentrega\", \"ADQUIR_telefono\", \"ADQUIR_obligaciones\", \"TOTAL_totalbruto\", \"TOTAL_baseimponible\", \"TOTAL_totalbrutoconimpuestos\",  " +
                                    "\"TOTAL_totaldescuento\", \"TOTAL_totalcargos\", \"TOTAL_totalanticipos\", \"TOTAL_totalapagar\", \"TOTAL_payableroundingamount\"  from " + SCHEMA + ".\"DATOS_GENERALES\"";*/

                string queryString = "select \"DOCNUM\", \"idnumeracion\", \"numero\", \"idambiente\", \"fechadocumento\", \"fechavencimiento\", \"tipofactura\", \"tipooperacion\", \"notas\", \"moneda\", \"cufe\", \"idreporte\", \"correocopia\", \"FP_idmetodopago\", " +
                                   "\"FP_idmediopago\", \"FP_fechavencimiento\", \"FP_identificador\", \"FP_dias\", \"idconceptonota\", \"REF_idnumeracion\", \"REF_numero\", \"ENT_fecha\", \"ENT_idciudad\",  " +
                                   "\"ENT_direccion\", \"ENT_codigopostal\", \"ENT_TRANS_idtipopersona\", \"ENT_TRANS_idactividadeconomica\", \"ENT_TRANS_nombrecomercial\", \"ENT_TRANS_idciudad\", \"ENT_TRANS_direccion\",  " +
                                   "\"ENT_TRANS_codigopostal\", \"ENT_TRANS_nombres\", \"ENT_TRANS_apellidos\", \"ENT_TRANS_idtipodocumentoidentidad\", \"ENT_TRANS_identificacion\", \"ENT_TRANS_digitoverificacion\",  " +
                                   "\"ENT_TRANS_idtiporegimen\", \"ENT_TRANSDF_Idciudad\", \"ENT_TRANSDF_direccion\", \"ENT_TRANSDF_codigopostal\", \"ENT_TRANS_matriculamercantil\", \"ENT_TRANS_emailcontacto\", \"ENT_TRANS_emailentrega\",  " +
                                   "\"ENT_TRANS_telefono\", \"ENT_TRANS_obligaciones\", \"ENT_metododepago\", \"ENT_condicionesdeentrega\", \"FACT_idtipopersona\", \"FACT_idactividadeconomica\", \"FACT_nombrecomercial\", \"FACT_idciudad\",  " +
                                   "\"FACT_direccion\", \"FACT_codigopostal\", \"FACT_nombres\", \"FACT_apellidos\", \"FACT_idtipodocumentoidentidad\", \"FACT_identificacion\", \"FACT_digitoverificacion\", \"FACT_idtiporegimen\",  " +
                                   "\"FACT_DIR_idciudad\", \"FACT_DIR_direccion\", \"FACT_DIR_codigopostal\", \"FACT_matriculamercantil\", \"FACT_emailcontacto\", \"FACT_emailentrega\", \"FACT_telefono\", \"FACT_obligaciones\",  " +
                                   "\"ADQUIR_idtipopersona\", \"ADQUIR_idactividadeconomica\", \"ADQUIR_nombrecomercial\", \"ADQUIR_idciudad\", \"ADQUIR_direccion\", \"ADQUIR_codigopostal\", \"ADQUIR_nombres\", \"ADQUIR_apellidos\",  " +
                                   "\"ADQUIR_idtipodocumentoidentidad\", \"ADQUIR_identificacion\", \"ADQUIR_digitoverificacion\", \"ADQUIR_idtiporegimen\", \"ADQUIR_DIR_idciudad\", \"ADQUIR_DIR_direccion\", \"ADQUIR_DIR_codigopostal\",  " +
                                   "\"ADQUIR_matriculamercantil\", \"ADQUIR_emailcontacto\", \"ADQUIR_emailentrega\", \"ADQUIR_telefono\", \"ADQUIR_obligaciones\", \"ORDENC_codigo\", \"ORDENC_fecha\", \"TOTAL_totalbruto\", \"TOTAL_baseimponible\", \"TOTAL_totalbrutoconimpuestos\",  " +
                                   "\"TOTAL_totaldescuento\", \"TOTAL_totalcargos\", \"TOTAL_totalanticipos\", \"TOTAL_totalapagar\", \"TOTAL_payableroundingamount\"  from " + SCHEMA + ".\"DATOS_GENERALES\"";

                /*                string queryString = "SELECT * FROM( " +
                "SELECT CONCAT((SELECT T0.\"Remark\" FROM \""+ this.SCHEMA +"\".NNM1 T0 WHERE T0.\"Series\"=L0.\"Series\"), CONCAT ('-', CAST(CAST(RIGHT(L0.\"DocNum\",6) AS NUMERIC(19)) AS NVARCHAR(19)))) \"DOCNUM\", " +
                "(SELECT T0.\"Remark\" FROM \""+ this.SCHEMA +"\".NNM1 T0 WHERE T0.\"Series\"=L0.\"Series\") \"idnumeracion\",  " +
                "CAST(RIGHT(L0.\"DocNum\",6) AS NUMERIC(19)) \"numero\",  " +
                "'1' \"idambiente\",                     " +
                "CAST(TO_VARCHAR(L0.\"DocDate\", 'YYYY-MM-DD hh:mi:ss') AS NVARCHAR(20)) \"fechadocumento\",  " +
                "CAST(TO_VARCHAR(L0.\"DocDueDate\", 'YYYY-MM-DD hh:mi:ss') AS NVARCHAR(20)) \"fechavencimiento\", " +
                //"CAST(TO_VARCHAR(CAST('20210921' AS DATE)) AS NVARCHAR(20)) \"fechadocumento\",  " +
                //"CAST(TO_VARCHAR(CAST('20210921' AS DATE)) AS NVARCHAR(20)) \"fechavencimiento\", " +
                "L0.\"U_SCL_FE_TiposdeFactura\" \"tipofactura\",  " +
                "IFNULL(L0.\"U_SCL_FE_TiposdeOperacion\",'') \"tipooperacion\",  " +


                                "L0.\"Comments\" \"notas\",  " +
                                //"'0' \"fechaimpuestos\",  " +
                                "L8.\"DocCurrCod\" \"moneda\",  " +
                                "'0' \"cufe\",  " +
                                "IFNULL((SELECT T0.\"U_SCL_FE_IDREPO\" FROM \""+ this.SCHEMA +"\".\"@SCL_FE_CONFNUM\" T0 WHERE T0.\"U_SCL_FE_SERIENUM\" = (SELECT T0.\"Remark\" FROM \""+ this.SCHEMA +"\".NNM1 T0 WHERE T0.\"Series\"=L0.\"Series\")),'') \"idreporte\",  " +

                                "'0' \"correocopia\", " +
                                "CASE WHEN \"PymntGroup\"='Contado' THEN '1' ELSE '2' END \"FP_idmetodopago\", " +
                                "'0' \"FP_idmediopago\",  " +
                                "CAST(TO_VARCHAR(L0.\"DocDueDate\", 'YYYY-MM-DD hh:mi:ss') AS NVARCHAR(20)) \"FP_fechavencimiento\", " +
                                //"CAST(TO_VARCHAR(CAST('20210921' AS DATE)) AS NVARCHAR(20)) \"FP_fechavencimiento\", " +
                                "'0' \"FP_identificador\", " +
                                "L3.\"ExtraDays\" \"FP_dias\", " +
                               // "'0' \"PF_fechainicial\",  " +
                               // "'0' \"PF_fechafinal\", " +
                                "'0' \"idconceptonota\", " +
                                "'0' \"REF_idnumeracion\", " +
                                "'0' \"REF_numero\", " +
                                "'0' \"ENT_fecha\", " +
                                "'0' \"ENT_idciudad\",    " +
                                "'0' \"ENT_direccion\",  " +
                                "'0' \"ENT_codigopostal\", " +
                                "'0' \"ENT_TRANS_idtipopersona\",  " +
                                "'0' \"ENT_TRANS_idactividadeconomica\",  " +
                                "'0' \"ENT_TRANS_nombrecomercial\",  " +
                                "'0' \"ENT_TRANS_idciudad\",  " +
                                "'0' \"ENT_TRANS_direccion\",  " +
                                "'0' \"ENT_TRANS_codigopostal\",  " +
                                "'0' \"ENT_TRANS_nombres\",  " +
                                "'0' \"ENT_TRANS_apellidos\",  " +
                                "'0' \"ENT_TRANS_idtipodocumentoidentidad\",  " +
                                "'0' \"ENT_TRANS_identificacion\", " +
                                "'0' \"ENT_TRANS_digitoverificacion\", " +
                                "'0' \"ENT_TRANS_idtiporegimen\",  " +
                                "'0' \"ENT_TRANSDF_Idciudad\",  " +
                                "'0' \"ENT_TRANSDF_direccion\",  " +
                                "'0' \"ENT_TRANSDF_codigopostal\", " +
                                "'0' \"ENT_TRANS_matriculamercantil\",  " +
                                "'0' \"ENT_TRANS_emailcontacto\",  " +
                                "'0' \"ENT_TRANS_emailentrega\", " +
                                "'0' \"ENT_TRANS_telefono\",  " +
                                "'0' \"ENT_TRANS_obligaciones\",  " +
                                "'0' \"ENT_metododepago\", " +
                                "'0' \"ENT_condicionesdeentrega\", " +
                                "'0' \"FACT_idtipopersona\", " +
                                "'0' \"FACT_idactividadeconomica\", " +
                                "L9.\"CompnyName\" \"FACT_nombrecomercial\", " +
                                "L6.\"U_SCL_CodigoMun\" \"FACT_idciudad\", " +
                                "(SELECT \"Street\" FROM \""+ this.SCHEMA +"\".ADM1) \"FACT_direccion\", " +
                                "'0' \"FACT_codigopostal\", " +             //56
                                "L9.\"CompnyName\" \"FACT_nombres\", " +
                                "'0' \"FACT_apellidos\", " +
                                "L9.\"TaxIdNum3\" \"FACT_idtipodocumentoidentidad\", " +
                                "left(L9.\"TaxIdNum\",9) \"FACT_identificacion\", " +
                                "RIGHT(L9.\"TaxIdNum\",1) \"FACT_digitoverificacion\", " +
                                "'0' \"FACT_idtiporegimen\", " +
                                "'11001' \"FACT_DIR_idciudad\", " +
                                "'0' \"FACT_DIR_direccion\", " +
                                "'0' \"FACT_DIR_codigopostal\", " +
                                "'0' \"FACT_matriculamercantil\", " +
                                "L9.\"E_Mail\" \"FACT_emailcontacto\", " +
                                "L9.\"E_Mail\" \"FACT_emailentrega\", " +
                                "'0' \"FACT_telefono\", " +
                                "'0' \"FACT_obligaciones\", " +
                                "'0' \"ADQUIR_idtipopersona\",  " +
                                "'0' \"ADQUIR_idactividadeconomica\",  " +
                                "L0.\"CardName\" \"ADQUIR_nombrecomercial\",  " +
                                "(SELECT T0.\"U_SCL_CodigoMun\" FROM \""+ this.SCHEMA +"\".OCRD T0 WHERE T0.\"CardCode\" =L0.\"CardCode\") \"ADQUIR_idciudad\", " +
                                "L6.\"Address\" \"ADQUIR_direccion\",  " +
                                "'000000' \"ADQUIR_codigopostal\",  " +
                                "L0.\"CardName\" \"ADQUIR_nombres\", " +
                                "'0' \"ADQUIR_apellidos\", " +
                                "L6.\"U_SCL_TipoDoc\" \"ADQUIR_idtipodocumentoidentidad\",  " +
                                "CASE WHEN LOCATE(L6.\"LicTradNum\", '-' )=0 THEN L0.\"LicTradNum\" ELSE LEFT(L6.\"LicTradNum\",LOCATE(L6.\"LicTradNum\", '-')-1) end \"ADQUIR_identificacion\",  " +
                                "CASE WHEN LOCATE(L6.\"LicTradNum\", '-' )=0 THEN '' ELSE RIGHT(L6.\"LicTradNum\",1) END \"ADQUIR_digitoverificacion\",  " +
                                "'0' \"ADQUIR_idtiporegimen\", " +
                                "L6.\"U_SCL_CodigoMun\" \"ADQUIR_DIR_idciudad\",  " +
                                "'0' \"ADQUIR_DIR_direccion\",  " +
                                "'0' \"ADQUIR_DIR_codigopostal\",  " +
                                "'0' \"ADQUIR_matriculamercantil\", " +
                                "L6.\"E_Mail\" \"ADQUIR_emailcontacto\", " +
                                "L6.\"E_Mail\" \"ADQUIR_emailentrega\",  " +
                                "L6.\"Phone1\" \"ADQUIR_telefono\", " +
                                "'R-99-PN' \"ADQUIR_obligaciones\", " +
                                 "CAST(CAST(L7.\"SUBTOTAL\" AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_totalbruto\",  " +
                 "CAST(CAST(IFNULL(L10.\"TOTAL_BASEIMP\",0) AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_baseimponible\",  " +
                 "CAST(CAST(L7.\"SUBTOTAL\"+L7.\"TOTAL_IMPUESTO\" AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_totalbrutoconimpuestos\",  " +
                 "CAST(CASE WHEN CAST(CASE WHEN L0.\"DocCur\" IN('COP','$') THEN L0.\"DiscSum\" ELSE L0.\"DiscSumFC\" END AS NUMERIC(19,2))>0 then CAST(CASE WHEN L0.\"DocCur\" IN('COP','$') THEN ABS(L0.\"DiscSum\") ELSE ABS(L0.\"DiscSumFC\") END AS NUMERIC(19,2)) ELSE '0' END AS NVARCHAR(23)) \"TOTAL_totaldescuento\",  " +
                 "CAST(CAST('0' AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_totalcargos\",  " +
                 "CAST(CAST('0' AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_totalanticipos\",  " +
                 "CAST(CAST(L7.\"SUBTOTAL\"+L7.\"TOTAL_IMPUESTO\" - IFNULL(L0.\"DiscSum\",0) AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_totalapagar\",  " +
                 "'0' \"TOTAL_payableroundingamount\" " +

                 "FROM \""+ this.SCHEMA +"\".OINV L0 JOIN \""+ this.SCHEMA +"\".NNM1 L1 ON L0.\"Series\"=L1.\"Series\" JOIN \""+ this.SCHEMA +"\".OADM L2 ON 1=1 LEFT JOIN \""+ this.SCHEMA +"\".OCTG L3 ON L0.\"GroupNum\" = L3.\"GroupNum\" " +
                 "LEFT JOIN(SELECT T0.\"DocEntry\",T1.\"DocDate\" FROM \""+ this.SCHEMA +"\".INV1 T0 JOIN \""+ this.SCHEMA +"\".ODLN T1 ON T0.\"BaseType\"=T1.\"ObjType\" AND T0.\"BaseEntry\"=T1.\"DocEntry\" WHERE T0.\"VisOrder\"=0) L4 ON L4.\"DocEntry\"=L0.\"DocEntry\" " +
                 "LEFT JOIN \""+ this.SCHEMA +"\".OCRD L6 ON L0.\"CardCode\"=L6.\"CardCode\" " +
                 "LEFT JOIN(SELECT \"DocEntry\",SUM(CASE WHEN \"Currency\" IN('COP','$') THEN \"LineTotal\" ELSE \"TotalFrgn\" END) \"SUBTOTAL\",  " +
                 "SUM(CASE WHEN \"Currency\" IN('COP','$') THEN \"LineTotal\" * ABS(\"VatPrcnt\"/100) ELSE \"LineVatlF\" * ABS(\"VatPrcnt\"/100) END) \"TOTAL_IMPUESTO\" " +
                 "FROM \""+ this.SCHEMA +"\".INV1 GROUP BY \"DocEntry\") L7 ON L0.\"DocEntry\"=L7.\"DocEntry\" LEFT JOIN \""+ this.SCHEMA +"\".OCRN L8 ON L0.\"DocCur\"=L8.\"CurrCode\" LEFT JOIN \""+ this.SCHEMA +"\".OADM L9 ON 1=1 " +
                 "LEFT JOIN (SELECT \"DocEntry\", SUM(\"LineTotal\") \"TOTAL_BASEIMP\" FROM \"" + this.SCHEMA + "\".INV1 WHERE \"VatPrcnt\" <> 0 GROUP BY \"DocEntry\") L10 ON L0.\"DocEntry\" = L10.\"DocEntry\" " +
                 "WHERE L0.\"DocSubType\"='--'  " +
                 "AND L0.\"DocDate\">'20210101' " +

                 //NOTACREDITO

                 "UNION ALL " +
                   "SELECT CONCAT((SELECT T0.\"Remark\" FROM \""+ this.SCHEMA +"\".NNM1 T0 WHERE T0.\"Series\"=L0.\"Series\"), CONCAT ('-', CAST(CAST(RIGHT(L0.\"DocNum\",6) AS NUMERIC(19)) AS NVARCHAR(19)))) \"DOCNUM\", " +
                   "(SELECT T0.\"Remark\" FROM \""+ this.SCHEMA +"\".NNM1 T0 WHERE T0.\"Series\"=L0.\"Series\") \"idnumeracion\",  " +
                   "CAST(RIGHT(L0.\"DocNum\",6) AS NUMERIC(19)) \"numero\",  " +
                   "'1' \"idambiente\",                       " +
                   "CAST(TO_VARCHAR(L0.\"DocDate\", 'YYYY-MM-DD hh:mi:ss') AS NVARCHAR(20)) \"fechadocumento\",  " +
                   "CAST(TO_VARCHAR(L0.\"DocDueDate\", 'YYYY-MM-DD hh:mi:ss') AS NVARCHAR(20)) \"fechavencimiento\", " +
                   "'' \"tipofactura\",     " +
                   "' ' \"tipooperacion\",  " +


                 "L0.\"Comments\"  \"notas\",  " +
                // "'0' \"fechaimpuestos\",  " +
                 "L8.\"DocCurrCod\" \"moneda\",  " +
                 "'0' \"cufe\",  " +
                 "IFNULL((SELECT T0.\"U_SCL_FE_IDREPO\" FROM \""+ this.SCHEMA +"\".\"@SCL_FE_CONFNUM\" T0 WHERE T0.\"U_SCL_FE_SERIENUM\" = (SELECT T0.\"Remark\" FROM \""+ this.SCHEMA +"\".NNM1 T0 WHERE T0.\"Series\"=L0.\"Series\")),'') \"idreporte\",  " +
                 "'0' \"correocopia\", " +
                 "CASE WHEN \"PymntGroup\"='Contado' THEN '1' ELSE '2' END \"FP_idmetodopago\", " +
                 "'0' \"FP_idmediopago\",  " +
                 "CAST(TO_VARCHAR(L0.\"DocDueDate\", 'YYYY-MM-DD hh:mi:ss') AS NVARCHAR(20)) \"FP_fechavencimiento\", " +
                 "'0' \"FP_identificador\", " +
                 "L3.\"ExtraDays\" \"FP_dias\", " +
              //   "'0' \"PF_fechainicial\",  " +
               //  "'0' \"PF_fechafinal\", " +
                 "L0.\"U_SCL_FE_ConceptosNC\" \"idconceptonota\", " +
                 //              "RIGHT(IFNULL(L0.\"U_SCL_FE_FACTURA\",''),6) \"REF_idnumeracion\", " +
                 "(SELECT T0.\"Remark\" FROM \"" + this.SCHEMA + "\".NNM1 T0 JOIN \"" + this.SCHEMA + "\".OINV F1 ON T0.\"Series\"=F1.\"Series\" WHERE \"DocEntry\" = L0.\"U_SCL_FE_FACTURA\") \"REF_idnumeracion\", " +
                 "(SELECT F1.\"DocNum\" FROM \"" + this.SCHEMA + "\".OINV F1 WHERE F1.\"DocEntry\" = L0.\"U_SCL_FE_FACTURA\") \"REF_numero\", " +
                 "'0' \"ENT_fecha\", " +
                 "'0' \"ENT_idciudad\",    " +
                 "'0' \"ENT_direccion\",  " +
                 "'0' \"ENT_codigopostal\", " +
                 "'0' \"ENT_TRANS_idtipopersona\",  " +
                 "'0' \"ENT_TRANS_idactividadeconomica\",  " +
                 "'0' \"ENT_TRANS_nombrecomercial\",  " +
                 "'0' \"ENT_TRANS_idciudad\",  " +
                 "'0' \"ENT_TRANS_direccion\",  " +
                 "'0' \"ENT_TRANS_codigopostal\",  " +
                 "'0' \"ENT_TRANS_nombres\",  " +
                 "'0' \"ENT_TRANS_apellidos\",  " +
                 "'0' \"ENT_TRANS_idtipodocumentoidentidad\",  " +
                 "'0' \"ENT_TRANS_identificacion\", " +
                 "'0' \"ENT_TRANS_digitoverificacion\", " +
                 "'0' \"ENT_TRANS_idtiporegimen\",  " +
                 "'0' \"ENT_TRANSDF_Idciudad\",  " +
                 "'0' \"ENT_TRANSDF_direccion\",  " +
                 "'0' \"ENT_TRANSDF_codigopostal\", " +
                 "'0' \"ENT_TRANS_matriculamercantil\",  " +
                 "'0' \"ENT_TRANS_emailcontacto\",  " +
                 "'0' \"ENT_TRANS_emailentrega\", " +
                 "'0' \"ENT_TRANS_telefono\",  " +
                 "'0' \"ENT_TRANS_obligaciones\",  " +
                 "'0' \"ENT_metododepago\", " +
                 "'0' \"ENT_condicionesdeentrega\", " +
                 "'0' \"FACT_idtipopersona\", " +
                 "'0' \"FACT_idactividadeconomica\", " +
                 "L9.\"CompnyName\" \"FACT_nombrecomercial\", " +
                 "L6.\"U_SCL_CodigoMun\" \"FACT_idciudad\", " +
                 "(SELECT \"Street\" FROM \""+ this.SCHEMA +"\".ADM1) \"FACT_direccion\", " +
                 "'0' \"FACT_codigopostal\", " +
                 "L9.\"CompnyName\" \"FACT_nombres\", " +
                 "'0' \"FACT_apellidos\", " +
                 "L9.\"TaxIdNum3\" \"FACT_idtipodocumentoidentidad\", " +
                 "left(L9.\"TaxIdNum\",9) \"FACT_identificacion\", " +
                 "RIGHT(L9.\"TaxIdNum\",1) \"FACT_digitoverificacion\", " +
                 "'0' \"FACT_idtiporegimen\", " +
                 "'11001' \"FACT_DIR_idciudad\", " +
                 "'0' \"FACT_DIR_direccion\", " +
                 "'0' \"FACT_DIR_codigopostal\", " +
                 "'0' \"FACT_matriculamercantil\", " +
                 "L9.\"E_Mail\" \"FACT_emailcontacto\", " +
                 "L9.\"E_Mail\" \"FACT_emailentrega\", " +
                 "'0' \"FACT_telefono\", " +
                 "'0' \"FACT_obligaciones\", " +
                 "'0' \"ADQUIR_idtipopersona\",  " +
                 "'0' \"ADQUIR_idactividadeconomica\",  " +
                 "L0.\"CardName\" \"ADQUIR_nombrecomercial\",  " +
                 "(SELECT T0.\"U_SCL_CodigoMun\" FROM \""+ this.SCHEMA +"\".OCRD T0 WHERE T0.\"CardCode\" =L0.\"CardCode\") \"ADQUIR_idciudad\", " +
                 "L6.\"Address\" \"ADQUIR_direccion\",  " +
                 "'000000' \"ADQUIR_codigopostal\",  " +
                 "L0.\"CardName\" \"ADQUIR_nombres\", " +
                 "'0' \"ADQUIR_apellidos\", " +
                 "L6.\"U_SCL_TipoDoc\" \"ADQUIR_idtipodocumentoidentidad\",  " +
                 "CASE WHEN LOCATE(L6.\"LicTradNum\", '-' )=0 THEN L0.\"LicTradNum\" ELSE LEFT(L6.\"LicTradNum\",LOCATE(L6.\"LicTradNum\", '-' )-1) end \"ADQUIR_identificacion\",  " +
                 "CASE WHEN LOCATE(L6.\"LicTradNum\", '-' )=0 THEN '' ELSE RIGHT(L6.\"LicTradNum\",1) END \"ADQUIR_digitoverificacion\",  " +
                 "'0' \"ADQUIR_idtiporegimen\", " +
                 "L6.\"U_SCL_CodigoMun\" \"ADQUIR_DIR_idciudad\",  " +
                 "L6.\"Address\" \"ADQUIR_DIR_direccion\",  " +
                 "'0' \"ADQUIR_DIR_codigopostal\",  " +
                 "'0' \"ADQUIR_matriculamercantil\", " +
                 "L6.\"E_Mail\" \"ADQUIR_emailcontacto\", " +
                 "L6.\"E_Mail\" \"ADQUIR_emailentrega\",  " +
                 "L6.\"Phone1\" \"ADQUIR_telefono\", " +
                 "'R-99-PN' \"ADQUIR_obligaciones\", " +
                 "CAST(CAST(L7.\"SUBTOTAL\" AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_totalbruto\",  " +
                 "CAST(CAST(IFNULL(L10.\"TOTAL_BASEIMP\",0) AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_baseimponible\",  " +
                 "CAST(CAST(L7.\"SUBTOTAL\"+L7.\"TOTAL_IMPUESTO\" AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_totalbrutoconimpuestos\",  " +
                 "CAST(CASE WHEN CAST(CASE WHEN L0.\"DocCur\" IN('COP','$') THEN L0.\"DiscSum\" ELSE L0.\"DiscSumFC\" END AS NUMERIC(19,2))>0 then CAST(CASE WHEN L0.\"DocCur\" IN('COP','$') THEN ABS(L0.\"DiscSum\") ELSE ABS(L0.\"DiscSumFC\") END AS NUMERIC(19,2)) ELSE '0' END AS NVARCHAR(23)) \"TOTAL_totaldescuento\",  " +
                 "CAST(CAST('0' AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_totalcargos\",  " +
                 "CAST(CAST('0' AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_totalanticipos\",  " +
                 "CAST(CAST(L7.\"SUBTOTAL\"+L7.\"TOTAL_IMPUESTO\" - IFNULL(L0.\"DiscSum\",0) AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_totalapagar\",  " +
                 "'0' \"TOTAL_payableroundingamount\" " +

                   " FROM \""+ this.SCHEMA +"\".ORIN L0 JOIN \""+ this.SCHEMA +"\".NNM1 L1 ON L0.\"Series\"=L1.\"Series\" JOIN \""+ this.SCHEMA +"\".OADM L2 ON 1=1 LEFT JOIN \""+ this.SCHEMA +"\".OCTG L3 ON L0.\"GroupNum\" = L3.\"GroupNum\" " +
                   "LEFT JOIN \""+ this.SCHEMA +"\".OCRD L6 ON L0.\"CardCode\"=L6.\"CardCode\" " +
                   "LEFT JOIN(SELECT \"DocEntry\",SUM(CASE WHEN \"Currency\" IN('COP','$') THEN \"LineTotal\" ELSE \"TotalFrgn\" END) \"SUBTOTAL\",  " +
                   "SUM(CASE WHEN \"Currency\" IN('COP','$') THEN \"LineTotal\" * ABS(\"VatPrcnt\"/100) ELSE \"LineVatlF\" * ABS(\"VatPrcnt\"/100) END) \"TOTAL_IMPUESTO\" " +
                   "FROM \"" + this.SCHEMA +"\".RIN1 GROUP BY \"DocEntry\") L7 ON L0.\"DocEntry\"=L7.\"DocEntry\" LEFT JOIN \""+ this.SCHEMA +"\".OCRN L8 ON L0.\"DocCur\"=L8.\"CurrCode\" LEFT JOIN \""+ this.SCHEMA +"\".OADM L9 ON 1=1 " +
                   "LEFT JOIN (SELECT \"DocEntry\", SUM(\"LineTotal\") \"TOTAL_BASEIMP\" FROM \"" + this.SCHEMA + "\".RIN1 WHERE \"VatPrcnt\" <> 0 GROUP BY \"DocEntry\") L10 ON L0.\"DocEntry\" = L10.\"DocEntry\" " +
                   "WHERE L0.\"DocSubType\"='--'  " +
                   "AND L0.\"DocDate\">'20210101' " +

                   //NOTA DEBITO

                   "UNION ALL " +

 "SELECT CONCAT((SELECT T0.\"Remark\" FROM \""+ this.SCHEMA +"\".NNM1 T0 WHERE T0.\"Series\"=L0.\"Series\"), CONCAT ('-', CAST(CAST(RIGHT(L0.\"DocNum\",6) AS NUMERIC(19)) AS NVARCHAR(19)))) \"DOCNUM\", " +
 "(SELECT T0.\"Remark\" FROM \""+ this.SCHEMA +"\".NNM1 T0 WHERE T0.\"Series\"=L0.\"Series\") \"idnumeracion\",  " +
 "CAST(RIGHT(L0.\"DocNum\",6) AS NUMERIC(19)) \"numero\",  " +
 "'1' \"idambiente\",                     " +
 "CAST(TO_VARCHAR(L0.\"DocDate\", 'YYYY-MM-DD hh:mi:ss') AS NVARCHAR(20)) \"fechadocumento\",  " +
 "CAST(TO_VARCHAR(L0.\"DocDueDate\", 'YYYY-MM-DD hh:mi:ss') AS NVARCHAR(20)) \"fechavencimiento\", " +
 "L0.\"U_SCL_FE_TiposdeFactura\" \"tipofactura\",  " +
 "IFNULL(L0.\"U_SCL_FE_TiposdeOperacion\",'') \"tipooperacion\",  " +


                 "L0.\"Comments\" \"notas\",  " +
                 //"'0' \"fechaimpuestos\",  " +
                 "L8.\"DocCurrCod\" \"moneda\",  " +
                 "'0' \"cufe\",  " +
                 "IFNULL((SELECT T0.\"U_SCL_FE_IDREPO\" FROM \""+ this.SCHEMA +"\".\"@SCL_FE_CONFNUM\" T0 WHERE T0.\"U_SCL_FE_SERIENUM\" = (SELECT T0.\"Remark\" FROM \""+ this.SCHEMA +"\".NNM1 T0 WHERE T0.\"Series\"=L0.\"Series\")),'') \"idreporte\",  " +

                 "'0' \"correocopia\", " +
                 "CASE WHEN \"PymntGroup\"='Contado' THEN '1' ELSE '2' END \"FP_idmetodopago\", " +
                 "'0' \"FP_idmediopago\",  " +
                 "CAST(TO_VARCHAR(L0.\"DocDueDate\", 'YYYY-MM-DD hh:mi:ss') AS NVARCHAR(20)) \"FP_fechavencimiento\", " +
                 "'0' \"FP_identificador\", " +
                 "L3.\"ExtraDays\" \"FP_dias\", " +
   // "'0' \"PF_fechainicial\",  " +
   // "'0' \"PF_fechafinal\", " +
   "L0.\"U_SCL_FE_ConceptosND\" \"idconceptonota\", " +
   //"RIGHT(IFNULL(L0.\"U_SCL_FE_FACTURA\",''),6) \"REF_idnumeracion\", " +
   "L0.\"U_SCL_FE_FACTURA\" \"REF_idnumeracion\", " +
                 //"'0' \"REF_idnumeracion\", " +
                 "L0.\"U_SCL_FE_FACTURA\" \"REF_numero\", " +
                 "'0' \"ENT_fecha\", " +
                 "'0' \"ENT_idciudad\",    " +
                 "'0' \"ENT_direccion\",  " +
                 "'0' \"ENT_codigopostal\", " +
                 "'0' \"ENT_TRANS_idtipopersona\",  " +
                 "'0' \"ENT_TRANS_idactividadeconomica\",  " +
                 "'0' \"ENT_TRANS_nombrecomercial\",  " +
                 "'0' \"ENT_TRANS_idciudad\",  " +
                 "'0' \"ENT_TRANS_direccion\",  " +
                 "'0' \"ENT_TRANS_codigopostal\",  " +
                 "'0' \"ENT_TRANS_nombres\",  " +
                 "'0' \"ENT_TRANS_apellidos\",  " +
                 "'0' \"ENT_TRANS_idtipodocumentoidentidad\",  " +
                 "'0' \"ENT_TRANS_identificacion\", " +
                 "'0' \"ENT_TRANS_digitoverificacion\", " +
                 "'0' \"ENT_TRANS_idtiporegimen\",  " +
                 "'0' \"ENT_TRANSDF_Idciudad\",  " +
                 "'0' \"ENT_TRANSDF_direccion\",  " +
                 "'0' \"ENT_TRANSDF_codigopostal\", " +
                 "'0' \"ENT_TRANS_matriculamercantil\",  " +
                 "'0' \"ENT_TRANS_emailcontacto\",  " +
                 "'0' \"ENT_TRANS_emailentrega\", " +
                 "'0' \"ENT_TRANS_telefono\",  " +
                 "'0' \"ENT_TRANS_obligaciones\",  " +
                 "'0' \"ENT_metododepago\", " +
                 "'0' \"ENT_condicionesdeentrega\", " +
                 "'0' \"FACT_idtipopersona\", " +
                 "'0' \"FACT_idactividadeconomica\", " +
                 "L9.\"CompnyName\" \"FACT_nombrecomercial\", " +
                 "L6.\"U_SCL_CodigoMun\" \"FACT_idciudad\", " +
                 "(SELECT \"Street\" FROM \""+ this.SCHEMA +"\".ADM1) \"FACT_direccion\", " +
                 "'0' \"FACT_codigopostal\", " +             //56
                 "L9.\"CompnyName\" \"FACT_nombres\", " +
                 "'0' \"FACT_apellidos\", " +
                 "L9.\"TaxIdNum3\" \"FACT_idtipodocumentoidentidad\", " +
                 "left(L9.\"TaxIdNum\",9) \"FACT_identificacion\", " +
                 "RIGHT(L9.\"TaxIdNum\",1) \"FACT_digitoverificacion\", " +
                 "'0' \"FACT_idtiporegimen\", " +
                 "'11001' \"FACT_DIR_idciudad\", " +
                 "'0' \"FACT_DIR_direccion\", " +
                 "'0' \"FACT_DIR_codigopostal\", " +
                 "'0' \"FACT_matriculamercantil\", " +
                 "L9.\"E_Mail\" \"FACT_emailcontacto\", " +
                 "L9.\"E_Mail\" \"FACT_emailentrega\", " +
                 "'0' \"FACT_telefono\", " +
                 "'0' \"FACT_obligaciones\", " +
                 "'0' \"ADQUIR_idtipopersona\",  " +
                 "'0' \"ADQUIR_idactividadeconomica\",  " +
                 "L0.\"CardName\" \"ADQUIR_nombrecomercial\",  " +
                 "(SELECT T0.\"U_SCL_CodigoMun\" FROM \""+ this.SCHEMA +"\".OCRD T0 WHERE T0.\"CardCode\" =L0.\"CardCode\") \"ADQUIR_idciudad\", " +
                 "L6.\"Address\" \"ADQUIR_direccion\",  " +
                 "'000000' \"ADQUIR_codigopostal\",  " +
                 "L0.\"CardName\" \"ADQUIR_nombres\", " +
                 "'0' \"ADQUIR_apellidos\", " +
                 "L6.\"U_SCL_TipoDoc\" \"ADQUIR_idtipodocumentoidentidad\",  " +
                 "CASE WHEN LOCATE(L6.\"LicTradNum\", '-' )=0 THEN L0.\"LicTradNum\" ELSE LEFT(L6.\"LicTradNum\",LOCATE(L6.\"LicTradNum\", '-')-1) end \"ADQUIR_identificacion\",  " +
                 "CASE WHEN LOCATE(L6.\"LicTradNum\", '-' )=0 THEN '' ELSE RIGHT(L6.\"LicTradNum\",1) END \"ADQUIR_digitoverificacion\",  " +
                 "'0' \"ADQUIR_idtiporegimen\", " +
                 "L6.\"U_SCL_CodigoMun\" \"ADQUIR_DIR_idciudad\",  " +
                 "'0' \"ADQUIR_DIR_direccion\",  " +
                 "'0' \"ADQUIR_DIR_codigopostal\",  " +
                 "'0' \"ADQUIR_matriculamercantil\", " +
                 "L6.\"E_Mail\" \"ADQUIR_emailcontacto\", " +
                 "L6.\"E_Mail\" \"ADQUIR_emailentrega\",  " +
                 "L6.\"Phone1\" \"ADQUIR_telefono\", " +
                 "'R-99-PN' \"ADQUIR_obligaciones\", " +
                 "CAST(CAST(L7.\"SUBTOTAL\"-IFNULL(L0.\"DiscSum\",0) AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_totalbruto\",  " +
                 "CAST(CAST(IFNULL(L10.\"TOTAL_BASEIMP\",0) AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_baseimponible\",  " +
                 "CAST(CAST(L7.\"SUBTOTAL\"+L7.\"TOTAL_IMPUESTO\" AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_totalbrutoconimpuestos\",  " +
                 "CAST(CASE WHEN CAST(CASE WHEN L0.\"DocCur\" IN('COP','$') THEN L0.\"DiscSum\" ELSE L0.\"DiscSumFC\" END AS NUMERIC(19,2))>0 then CAST(CASE WHEN L0.\"DocCur\" IN('COP','$') THEN ABS(L0.\"DiscSum\") ELSE ABS(L0.\"DiscSumFC\") END AS NUMERIC(19,2)) ELSE '0' END AS NVARCHAR(23)) \"TOTAL_totaldescuento\",  " +
                 "CAST(CAST('0' AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_totalcargos\",  " +
                 "CAST(CAST('0' AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_totalanticipos\",  " +
                 "CAST(CAST(L7.\"SUBTOTAL\"+L7.\"TOTAL_IMPUESTO\" - IFNULL(L0.\"DiscSum\",0) AS NUMERIC(19,2)) AS NVARCHAR(23)) \"TOTAL_totalapagar\",  " +
                 "'0' \"TOTAL_payableroundingamount\" " +

                 "FROM \""+ this.SCHEMA +"\".OINV L0 JOIN \""+ this.SCHEMA +"\".NNM1 L1 ON L0.\"Series\"=L1.\"Series\" JOIN \""+ this.SCHEMA +"\".OADM L2 ON 1=1 LEFT JOIN \""+ this.SCHEMA +"\".OCTG L3 ON L0.\"GroupNum\" = L3.\"GroupNum\" " +
                 "LEFT JOIN(SELECT T0.\"DocEntry\",T1.\"DocDate\" FROM \""+ this.SCHEMA +"\".INV1 T0 JOIN \""+ this.SCHEMA +"\".ODLN T1 ON T0.\"BaseType\"=T1.\"ObjType\" AND T0.\"BaseEntry\"=T1.\"DocEntry\" WHERE T0.\"VisOrder\"=0) L4 ON L4.\"DocEntry\"=L0.\"DocEntry\" " +
                 "LEFT JOIN \""+ this.SCHEMA +"\".OCRD L6 ON L0.\"CardCode\"=L6.\"CardCode\" " +
                 "LEFT JOIN(SELECT \"DocEntry\",SUM(CASE WHEN \"Currency\" IN('COP','$') THEN \"LineTotal\" ELSE \"TotalFrgn\" END) \"SUBTOTAL\",  " +
                 "SUM(CASE WHEN \"Currency\" IN('COP','$') THEN \"LineTotal\" * ABS(\"VatPrcnt\"/100) ELSE \"LineVatlF\" * ABS(\"VatPrcnt\"/100) END) \"TOTAL_IMPUESTO\" " +
                 "FROM \"" + this.SCHEMA +"\".INV1 GROUP BY \"DocEntry\") L7 ON L0.\"DocEntry\"=L7.\"DocEntry\" LEFT JOIN \""+ this.SCHEMA +"\".OCRN L8 ON L0.\"DocCur\"=L8.\"CurrCode\" LEFT JOIN \""+ this.SCHEMA +"\".OADM L9 ON 1=1 "+
                 "LEFT JOIN (SELECT \"DocEntry\", SUM(\"LineTotal\") \"TOTAL_BASEIMP\" FROM \"" + this.SCHEMA + "\".INV1 WHERE \"VatPrcnt\" <> 0 GROUP BY \"DocEntry\") L10 ON L0.\"DocEntry\" = L10.\"DocEntry\" " +
                 "WHERE L0.\"DocSubType\"<>'--'  " +
   "AND L0.\"DocDate\">'20210101' " +
                     ")LL " +
                 "WHERE \"DOCNUM\" NOT IN(SELECT \"Code\" FROM \"" + this.SCHEMA + "\".\"@FE_INFO\") " +
                 "AND \"DOCNUM\" NOT IN(SELECT \"Code\" FROM \"" + this.SCHEMA + "\".\"@SCL_FE_ESTADO\" WHERE \"U_SCL_EstadoDoc\" <> 'M')";*/

                //using (connection = new SqlConnection(connectionString))
                //{
                //SqlCommand command = new SqlCommand(queryString, connection);
                try
                    {
                        return QueryResult(connectionString, queryString);
                    }
                    catch (SqlException exp)
                    {
                        CsvGeneratorLog.StoreLog($"{this.ToString()}_GetDocumentsData_query_sql  {exp.Message}", EventLogEntryType.Error);
                        return null;
                    }
                    catch (Exception ex)
                    {
                        CsvGeneratorLog.StoreLog($"{this.ToString()}_GetDocumentsData_query  {ex.Message}", EventLogEntryType.Error);
                        return null;
                    }
                //}
            }
            catch (Exception exp)
            {
                CsvGeneratorLog.StoreLog($"{this.ToString()}_GetDocumentsData  {exp.Message}", EventLogEntryType.Error);
                return null;
            }
            finally
            {
 //               connection.Close();
            }
        }


        /// <summary>
        /// Funcion para traer el listado de los anticipos correspondientes a los documentos que se procesaran, 
        /// el orden de los campos puede varias pero no se puede cambiar el nombre ni disminuir la cantidad
        /// </summary>
        /// <returns> Devuelve una datatable. Columnas son los campos del query y cada fila es uno de los Anticipos </returns>
        public DataTable GetAnticiposData()
        {
            try
            {
                string queryString = "select \"DOCNUM\", \"ANTICIPO_identificador\", \"ANTICIPO_valor\", \"ANTICIPO_fecha\" from " + SCHEMA + ".\"ANTICIPO\"";

                //using (connection = new SqlConnection(connectionString))
                //{
                //SqlCommand command = new SqlCommand(queryString, connection);
                try
                {
                    return QueryResult(connectionString, queryString);
                }
                    catch (Exception ex)
                    {
                        CsvGeneratorLog.StoreLog($"{this.ToString()}_GetAnticiposData_query  {ex.Message}", EventLogEntryType.Error);
                        return null;
                    }
                //}
            }
            catch (Exception exp)
            {
                CsvGeneratorLog.StoreLog($"{this.ToString()}_GetAnticiposData  {exp.Message}", EventLogEntryType.Error);
                return null;
            }
            finally
            {
  //              connection.Close();
            }
        }

        /// <summary>
        /// Funcion para traer el listado de los Descuentos correspondientes a los documentos que se procesaran, 
        /// el orden de los campos puede varias pero no se puede cambiar el nombre ni disminuir la cantidad
        /// </summary>
        /// <returns> Devuelve una datatable. Columnas son los campos del query y cada fila es uno de los Descuentos  </returns>
        public DataTable GetDescuentosData()
        {
            try
            {
                string queryString = "select \"DOCNUM\", \"DESC_idconcepto\", \"DESC_escargo\", \"DESC_descripcion\", \"DESC_porcentaje\", \"DESC_base\", \"DESC_valor\" from " + SCHEMA + ".\"DESCUENTOS\"";

                //using (connection = new SqlConnection(connectionString))
                //{
                //SqlCommand command = new SqlCommand(queryString, connection);
                try
                {
                    return QueryResult(connectionString, queryString);
                }
                    catch (Exception ex)
                    {
                        CsvGeneratorLog.StoreLog($"{this.ToString()}_GetDescuentosData_query  {ex.Message}", EventLogEntryType.Error);
                        return null;
                    }
                //}
            }
            catch (Exception exp)
            {
                CsvGeneratorLog.StoreLog($"{this.ToString()}_GetDescuentosData  {exp.Message}", EventLogEntryType.Error);
                return null;
            }
            finally
            {
  //              connection.Close();
            }
        }

        /// <summary>
        /// Funcion para traer el listado de los impuestos correspondientes a los documentos que se procesaran, 
        /// el orden de los campos puede varias pero no se puede cambiar el nombre ni disminuir la cantidad
        /// </summary>
        /// <returns> Devuelve una datatable. Columnas son los campos del query y cada fila es uno de los Impuestos </returns>
        public DataTable GetImpuestosData()
        {
            try
            {
                string queryString = "select \"DOCNUM\", \"IMPUESTOS_idimpuesto\", \"IMPUESTOS_base\", \"IMPUESTOS_factor\", \"IMPUESTOS_estarifaunitaria\", \"IMPUESTOS_valor\" from " + SCHEMA + ".\"IMPUESTOS\"";

                //using (connection = new SqlConnection(connectionString))
                //{
                //SqlCommand command = new SqlCommand(queryString, connection);
                try
                {
                    return QueryResult(connectionString, queryString);
                }
                    catch (Exception ex)
                    {
                        CsvGeneratorLog.StoreLog($"{this.ToString()}_GetImpuestosData_query  {ex.Message}", EventLogEntryType.Error);
                        return null;
                    }
                //}
            }
            catch (Exception exp)
            {
                CsvGeneratorLog.StoreLog($"{this.ToString()}_GetImpuestosData  {exp.Message}", EventLogEntryType.Error);
                return null;
            }
            finally
            {
 //               connection.Close();
            }
        }


        /// <summary>
        /// Funcion para traer el listado de los impuestos correspondientes a los documentos que se procesaran, 
        /// el orden de los campos puede varias pero no se puede cambiar el nombre ni disminuir la cantidad
        /// </summary>
        /// <returns> Devuelve una datatable. Columnas son los campos del query y cada fila es uno de los Impuestos </returns>
        public DataTable GetDatosExtraData()
        {
            try
            {
                string queryString = "SELECT \"DOCNUM\", \"RETENCIONES_idretencion\", \"RETENCIONES_base\", \"RETENCIONES_factor\", \"RETENCIONES_valor\", \"RETENCIONES_tipo\" FROM " + SCHEMA + ".\"DATOS_EXTRAS\"";

                //using (connection = new SqlConnection(connectionString))
                //{
                //SqlCommand command = new SqlCommand(queryString, connection);
                try
                {
                    return QueryResult(connectionString, queryString);
                }
                    catch (Exception ex)
                    {
                        CsvGeneratorLog.StoreLog($"{this.ToString()}_GetDatosExtraData_query  {ex.Message}", EventLogEntryType.Error);
                        return null;
                    }
                //}
            }
            catch (Exception exp)
            {
                CsvGeneratorLog.StoreLog($"{this.ToString()}_GetDatosExtraData  {exp.Message}", EventLogEntryType.Error);
                return null;
            }
            finally
            {
 //               connection.Close();
            }
        }


        /// <summary>
        /// Funcion para traer el listado de los anticipos correspondientes a los documentos que se procesaran, 
        /// el orden de los campos puede varias pero no se puede cambiar el nombre ni disminuir la cantidad
        /// </summary>
        /// <returns> Devuelve una datatable. Columnas son los campos del query y cada fila es uno de los Anticipos </returns>
        public DataTable GetItemsData()
        {
            try
            {
                /*string queryString = "select \"DOCNUM\", \"ITEMS_COD_idestandar\", \"ITEMS_COD_nombreestandar\", \"ITEMS_COD_codigo\", \"ITEMS_descripcion\", \"ITEMS_notas\", \"ITEMS_cantidad\", \"ITEMS_cantidadporempaque\", " +
                            "\"ITEMS_preciounitario\", \"ITEMS_unidaddemedida\", \"ITEMS_marca\", \"ITEMS_modelo\", \"ITEMS_codigovendedor\", \"ITEMS_subcodigovendedor\", \"ITEMS_idmandante\", \"ITEMS_regalo\", \"ITEMS_totalitem\", " +
                            "\"ITEMS_CARGO_idconcepto\", \"ITEMS_CARGO_escargo\", \"ITEMS_CARGO_descripcion\", \"ITEMS_CARGO_porcentaje\", \"ITEMS_CARGO_base\", \"ITEMS_CARGO_valor\", \"ITEMS_IMPUES_idimpuesto\", \"ITEMS_IMPUES_base\", " +
                            "\"ITEMS_IMPUES_factor\", \"ITEMS_IMPUES_estarifaunitaria\", \"ITEMS_IMPUES_valor\", \"ITEMS_DT_clave\", \"ITEMS_DT_valor\" from " + SCHEMA + ".\"ITEMS\"";
                */
                string queryString = "select \"DOCNUM\", \"ITEMS_COD_idestandar\", \"ITEMS_COD_nombreestandar\", \"ITEMS_COD_codigo\", \"ITEMS_descripcion\", \"ITEMS_notas\", \"ITEMS_cantidad\", \"ITEMS_cantidadporempaque\", " +
                            "\"ITEMS_preciounitario\", \"ITEMS_unidaddemedida\", \"ITEMS_marca\", \"ITEMS_modelo\", \"ITEMS_codigovendedor\", \"ITEMS_subcodigovendedor\", \"ITEMS_idmandante\", \"ITEMS_regalo\", \"ITEMS_totalitem\",  \"ITEMS_fechacompra\", \"ITEMS_formageneraciontransmision\", " +
                            "\"ITEMS_CARGO_idconcepto\", \"ITEMS_CARGO_escargo\", \"ITEMS_CARGO_descripcion\", \"ITEMS_CARGO_porcentaje\", \"ITEMS_CARGO_base\", \"ITEMS_CARGO_valor\", \"ITEMS_IMPUES_idimpuesto\", \"ITEMS_IMPUES_base\", " +
                            "\"ITEMS_IMPUES_factor\", \"ITEMS_IMPUES_estarifaunitaria\", \"ITEMS_IMPUES_valor\", \"ITEMS_DT_clave\", \"ITEMS_DT_valor\" from " + SCHEMA + ".\"ITEMS\"";
                //using (connection = new SqlConnection(connectionString))
                //{
                //SqlCommand command = new SqlCommand(queryString, connection);
                try
                {
                    return QueryResult(connectionString, queryString);
                }
                    catch (Exception ex)
                    {
                        CsvGeneratorLog.StoreLog($"{this.ToString()}_GetItemsData_query  {ex.Message}", EventLogEntryType.Error);
                        return null;
                    }
                //}
            }
            catch (Exception exp)
            {
                CsvGeneratorLog.StoreLog($"{this.ToString()}_GetItemsData  {exp.Message}", EventLogEntryType.Error);
                return null;
            }
            finally
            {
  //              connection.Close();
            }
        }

        /// <summary>
        /// Funcion para ejecutar query generico
        /// </summary>
        /// <param name="connection">Coneccion a la base de datos</param>
        /// <param name="queryString">Query a procesar</param>
        /// <returns> Devuelve una datatable con el resultado del query </returns>
        private DataTable QueryResult(string connection, string queryString)
        {
            HanaConnection hconn = new HanaConnection(connection);

            hconn.Open();

            //HanaDataAdapter da = new HanaDataAdapter(queryString, hconn);
            HanaCommand hc = new HanaCommand(queryString, hconn);
            DataTable dt = new DataTable();
            
            HanaDataReader hr = hc.ExecuteReader();
            //da.Fill(dt);
            dt.Load(hr);
            hr.Close();
            hconn.Close();
            return dt;
        }

        /// <summary>
        /// Funcion para insertar en la tabla de traza los documentos que se actualizaron 
        /// </summary>
        /// <param name="DocNum">Numero de documento a insertar</param>
        /// <param name="idDocumentoElectronico">Id del documento devuelto por el servicio web</param>
        /// <param name="cufe">valor Cufe a insertar en la tabla</param>
        /// <param name="qr_data">data devuelta por el servicio web</param>
        /// <returns> Void </returns>
        public void InsertSuccessData(string DocNum, string idDocumentoElectronico, string cufe, string qr_data)
        {
            try
            {
                string queryString = $"INSERT INTO " + SCHEMA + ".\"@FE_INFO\" (\"Code\", \"Name\", \"U_idDocElect\", \"U_CUFE\", \"U_QR_DATA\") VALUES ('"+DocNum+"', '"+DocNum+"', '"+idDocumentoElectronico+"', '"+cufe+"', '"+qr_data+"')";

                //using (connection = new SqlConnection(connectionString))
                //{
                // SqlCommand command = new SqlCommand(queryString, connection);
                HanaConnection hconn = new HanaConnection(connectionString);
                try
                    {
                    hconn.Open();
                    HanaCommand hc = new HanaCommand(queryString, hconn);
                    hc.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        CsvGeneratorLog.StoreLog($"{this.ToString()}_InsertSuccessData_query  {ex.Message}", EventLogEntryType.Error);
                     }
                //}
            }
            catch (Exception exp)
            {
                CsvGeneratorLog.StoreLog($"{this.ToString()}_InsertSuccessData  {exp.Message}", EventLogEntryType.Error);
             }
            finally
            {
  //              connection.Close();
            }
        }
        //INSERTAR INFORMACIÓN TABLA INTERMEDIA DE LOS DOCUMENTOS RECHAZADOS POR EL PROVEEDOR TECNOLOGICO
        public void InsertFailData(string DocNum, string numDoc, string log, string estado)
        {
            try
            {
                string queryString = $"INSERT INTO " + SCHEMA + ".\"@SCL_FE_ESTADO\" (\"Code\", \"Name\",\"U_SCL_NumeroDoc\", \"U_SCL_FechaEnv\", \"U_SCL_LogDoc\", \"U_SCL_EstadoDoc\") VALUES ('" + DocNum + "', '" + DocNum + "','" + numDoc + "', '" + DateTime.Now.ToString("yyyyMMdd") + "','" + log + "', '" + estado + "')";

                //using (connection = new SqlConnection(connectionString))
                //{
                // SqlCommand command = new SqlCommand(queryString, connection);
                HanaConnection hconn = new HanaConnection(connectionString);
                try
                {
                    hconn.Open();
                    HanaCommand hc = new HanaCommand(queryString, hconn);
                    hc.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    CsvGeneratorLog.StoreLog($"{this.ToString()}_InsertFailData_query  {ex.Message}" + " DocNum " + DocNum, EventLogEntryType.Error);
                }
                //}
            }
            catch (Exception exp)
            {
                CsvGeneratorLog.StoreLog($"{this.ToString()}_InsertFailData  {exp.Message}", EventLogEntryType.Error);
            }
            finally
            {
                //              connection.Close();
            }
        }

        //Consultar numeracióm
        public DataTable GetSeriesName(string DocNum)
        {
            try
            {
                string[] idNum = DocNum.Split('-');
                string id = idNum[0];

                string queryString = $"SELECT \"SeriesName\" FROM " + SCHEMA + ".\"NNM1\" WHERE \"Remark\" = '" + id + "'";

                //using (connection = new SqlConnection(connectionString))
                //{
                // SqlCommand command = new SqlCommand(queryString, connection);
                HanaConnection hconn = new HanaConnection(connectionString);
                try
                {
                    return QueryResult(connectionString, queryString);
                }
                catch (Exception ex)
                {
                    CsvGeneratorLog.StoreLog($"{this.ToString()}_GetSeriesName_query  {ex.Message}" + " DocNum " + DocNum, EventLogEntryType.Error);
                    return null;
                }
                //}
            }
            catch (Exception exp)
            {
                CsvGeneratorLog.StoreLog($"{this.ToString()}_GetSeriesName  {exp.Message}", EventLogEntryType.Error);
                return null;
            }
            finally
            {
                //              connection.Close();
            }
        }
        //Este metodo elimina los registros con estado 'M' de la tabla Documento Devueltos
        public void DeleteRowsM()
        {
            try
            {
                string queryString = $"DELETE FROM " + SCHEMA + ".\"@SCL_FE_ESTADO\" WHERE \"U_SCL_EstadoDoc\" = 'M'";

                HanaConnection hconn = new HanaConnection(connectionString);
                try
                {
                    hconn.Open();
                    HanaCommand hc = new HanaCommand(queryString, hconn);
                    hc.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    CsvGeneratorLog.StoreLog($"{this.ToString()}_DeleteRowsM_query  {ex.Message}", EventLogEntryType.Error);
                }
                //}
            }
            catch (Exception exp)
            {
                CsvGeneratorLog.StoreLog($"{this.ToString()}_DeleteRowsM  {exp.Message}", EventLogEntryType.Error);
            }
            finally
            {
                //              connection.Close();
            }
        }
    }
}
