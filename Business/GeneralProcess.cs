using Business.Soap;
using Log;
using Model;
using Model.Data;
using Model.Query;
using Model.XmlModel;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Business
{
    public class GeneralProcess : IGeneralProcess
    {
        private readonly IEventLogStore CsvGeneratorLog;
        private readonly ISoapProcess soapProcess;
        private readonly IXmlProcess xmlProcess;
        private readonly IGeneralDataGeneration generalDataGeneration;
        private readonly IDbQuery dbquery;

        public GeneralProcess(IEventLogStore csvGeneratorLog, ISoapProcess soapProcess, IXmlProcess xmlProcess, IGeneralDataGeneration _generalDataGeneration, IDbQuery _dbquery)
        {
            CsvGeneratorLog = csvGeneratorLog;
            this.soapProcess = soapProcess;
            this.xmlProcess = xmlProcess;
            this.generalDataGeneration = _generalDataGeneration;
            this.dbquery = _dbquery;
        }

        /// <summary>
        /// Proceso principal del apliocativo en el cual se llaman las demas funciones y es llamado por el servicio windows o el proyecto de prueba (aplicacion de consola)
        /// </summary>
        /// <returns> void </returns>
        public void MainProcess()
        {
            try
            {
                //Valida conectividad con el servicio web
                //SoapResponse pingResponse = soapProcess.ValidatePingConnection();
                //if (pingResponse.success)
                if (true)
                {
                   // CsvGeneratorLog.StoreLog($"Ping : {pingResponse.success}", EventLogEntryType.Error);
                    //inicia sesion en el servicio web y almacena el token
                    //SoapResponse sesionResponse = soapProcess.SoapInicioSesion();
                    //if (sesionResponse.success)
                    if (true)
                    {
                        //CsvGeneratorLog.StoreLog($"Sesión : {sesionResponse.success}", EventLogEntryType.Error);
                        //Trae la lista de documentos que se procesaran
                        List<Xmldocumento> XmlsToProcess = generalDataGeneration.GenerateDocumentList();
                        foreach (Xmldocumento doc in XmlsToProcess)
                        {
                            //se general el xml completo del documento y se devuelve en forma de string
                            string xmlString = xmlProcess.GetBase64Xml(doc);
                            //Se envia el xml generado al servicio web y se maneja las posibles respuestas
                            (SoapResponse xmlResponse, DataSoapResponseString xmlResponseString, DataSoapResponseObject xmlResponseObject) = soapProcess.SoapRegistrarDocumentoElectronico(xmlString);
                            if (xmlResponse.success)
                            {
                                //TODO: Guardar en base de datos
                                //Si la respuesta del servicio es true (success) se almacena en la base de datos
                                dbquery.InsertSuccessData(doc.DOCNUM, xmlResponseObject.idDocumentoElectronico, xmlResponseObject.cufe, xmlResponseObject.qr_data);
                                dbquery.DeleteRowsM();
                            }
                            else
                            {
                                dbquery.DeleteRowsM();
                                //Si la respuesta del servicio es false (fail) se almacena en el log y no se cambia estado del documento para futuros procesos}
                                CsvGeneratorLog.StoreLog($"Error al enviar el xml y el mensaje : {xmlResponse.msg}", EventLogEntryType.Warning);
                                //CsvGeneratorLog.StoreLog($"Error al enviar el xml y el mensaje : {xmlResponseString.salida}", EventLogEntryType.Warning);
                                DataTable res = dbquery.GetSeriesName(doc.DOCNUM);
                                if (res != null)
                                {
                                    string numeracion = res.Rows[0].ItemArray[0].ToString();
                                    dbquery.InsertFailData(doc.DOCNUM, numeracion + "" + doc.numero, xmlResponse.msg.Replace("'", ""), "R");
                                }
                            }
                        }
                        soapProcess.SoapCierreSesion();
                    }
                    else
                    {
                        //CsvGeneratorLog.StoreLog($"La respuesta del inicio de sesion es falsa: {sesionResponse.msg}", EventLogEntryType.Error);
                    }
                }
                else
                {
                    //CsvGeneratorLog.StoreLog($"La respuesta del ping es falsa: {pingResponse.msg}", EventLogEntryType.Error);
                }
            }
            catch (Exception exp)
            {
                soapProcess.SoapCierreSesion();
                CsvGeneratorLog.StoreLog($"{this.ToString()}_MainProcess  {exp.Message}", EventLogEntryType.Error);
            }
            finally
            {
                soapProcess.SoapCierreSesion();
            }
        }
    }
}
