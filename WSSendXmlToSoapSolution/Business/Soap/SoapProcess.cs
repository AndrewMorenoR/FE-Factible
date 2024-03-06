using Log;
using Model;
using Newtonsoft.Json;
using System;
using System.Configuration;
using System.Diagnostics;

namespace Business.Soap
{
    public class SoapProcess : ISoapProcess
    {
        private string token = "";
        private readonly IEventLogStore CsvGeneratorLog;
        private string SoapUser = ConfigurationSettings.AppSettings.Get("SoapUser").ToString();
        private string SoapPassword = ConfigurationSettings.AppSettings.Get("SoapPassword").ToString();
        public SoapProcess(IEventLogStore csvGeneratorLog)
        {
            CsvGeneratorLog = csvGeneratorLog;
        }

        /// <summary>
        /// Se ejecuta para validar la conectividad con el end point configurado
        /// </summary>
        /// <returns> Devuelve una objeto del tipo SoapResponse, el cual esta compuesto de la respuesta del WS </returns>
        public SoapResponse ValidatePingConnection()
        {
            try
            {
                WSFacturacion.FacturacionWebService connection = new WSFacturacion.FacturacionWebService();
                SoapResponseString response = JsonConvert.DeserializeObject<SoapResponseString>(connection.ping());
                return response;
            }
            catch (Exception exp)
            {
                CsvGeneratorLog.StoreLog($"{this.ToString()}_ValidatePingConnection  {exp.Message}", EventLogEntryType.Error);
                return new SoapResponse() { success = false, msg = exp.Message };
            }
        }

        /// <summary>
        /// Se ejecuta para iniciar sesion con el servicio web enviando usuario y contrasena
        /// </summary>
        /// <returns> Devuelve una objeto del tipo SoapResponse, el cual esta compuesto de la respuesta del WS y se almacena el token asignado para futuras consultas </returns>
        public SoapResponse SoapInicioSesion()
        {
            try
            {
                WSFacturacion.FacturacionWebService connection = new WSFacturacion.FacturacionWebService();

                SoapResponseString response = JsonConvert.DeserializeObject<SoapResponseString>(connection.autenticar(SoapUser, SoapPassword));
                this.token = response.data.salida.ToString();
                return response;
            }
            catch (Exception exp)
            {
                CsvGeneratorLog.StoreLog($"{this.ToString()}_SoapInicioSesion  {exp.Message}", EventLogEntryType.Error);
                return new SoapResponse() { success = false, msg = exp.Message };
            }
        }

        /// <summary>
        /// Se ejecuta para finalizar la sesion en el servicio web y no dejar activo token o sesion
        /// </summary>
        /// <returns> Devuelve una objeto del tipo SoapResponse, el cual esta compuesto de la respuesta del WS </returns>
        /// 
        public SoapResponse SoapCierreSesion()
        {
            try
            {
                WSFacturacion.FacturacionWebService connection = new WSFacturacion.FacturacionWebService();

                SoapResponseString response = JsonConvert.DeserializeObject<SoapResponseString>(connection.cerrarSesion(this.token));

                return response;
            }
            catch (Exception exp)
            {
                CsvGeneratorLog.StoreLog($"{this.ToString()}_SoapCierreSesion  {exp.Message}", EventLogEntryType.Error);
                return new SoapResponse() { success = false, msg = exp.Message };
            }
        }


        /// <summary>
        /// Se ejecuta para enviar el xml del documento con la estructura acordada, para ser procesada en el servicio Web
        /// </summary>
        /// <param name="Base64Xml">String con el xml codificado en base 64</param>
        /// <returns> Devuelve una objeto del tipo SoapResponse, el cual esta compuesto de la respuesta del WS, 
        /// devuelve DataSoapResponseString o DataSoapResponseObject dependiendo el estado de la respuesta 
        /// </returns>
        public (SoapResponse, DataSoapResponseString, DataSoapResponseObject) SoapRegistrarDocumentoElectronico(string Base64Xml)
        {
            try
            {
                WSFacturacion.FacturacionWebService connection = new WSFacturacion.FacturacionWebService();
                var dataResponse = connection.registrarDocumentoElectronico_Generar_FuenteXML(this.token, Base64Xml, false);
                SoapResponse response = JsonConvert.DeserializeObject<SoapResponse>(dataResponse);

                //Valida el estado de la respuesta para devolver un tipo de dato diferente si fue success o fail
                if (response.success)
                {
                    return (response, null, JsonConvert.DeserializeObject<SoapResponseObject>(dataResponse).data[0]);
                }
                else
                {
                    return (response, JsonConvert.DeserializeObject<SoapResponseString>(dataResponse).data, null);
                }
            }
            catch (Exception exp)
            {
                CsvGeneratorLog.StoreLog($"{this.ToString()}_SoapRegistrarDocumentoElectronico  {exp.Message}", EventLogEntryType.Error);
                return (new SoapResponse() { success = false, msg = exp.Message }, null, null);
            }
        }

        
    }
}
