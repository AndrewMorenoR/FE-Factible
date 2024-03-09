using Model;

namespace Business.Soap
{
    public interface ISoapProcess
    {
        /// <summary>
        /// Se ejecuta para validar la conectividad con el end point configurado
        /// </summary>
        /// <returns> Devuelve una objeto del tipo SoapResponse, el cual esta compuesto de la respuesta del WS </returns>
        SoapResponse ValidatePingConnection();

        /// <summary>
        /// Se ejecuta para iniciar sesion con el servicio web enviando usuario y contrasena
        /// </summary>
        /// <returns> Devuelve una objeto del tipo SoapResponse, el cual esta compuesto de la respuesta del WS y se almacena el token asignado para futuras consultas </returns>
        SoapResponse SoapInicioSesion();

        /// <summary>
        /// Se ejecuta para finalizar la sesion en el servicio web y no dejar activo token o sesion
        /// </summary>
        /// <returns> Devuelve una objeto del tipo SoapResponse, el cual esta compuesto de la respuesta del WS </returns>
        SoapResponse SoapCierreSesion();

        /// <summary>
        /// Se ejecuta para enviar el xml del documento con la estructura acordada, para ser procesada en el servicio Web
        /// </summary>
        /// <param name="Base64Xml">String con el xml codificado en base 64</param>
        /// <returns> Devuelve una objeto del tipo SoapResponse, el cual esta compuesto de la respuesta del WS, 
        /// devuelve DataSoapResponseString o DataSoapResponseObject dependiendo el estado de la respuesta 
        /// </returns>
        (SoapResponse, DataSoapResponseString, DataSoapResponseObject) SoapRegistrarDocumentoElectronico(string Base64Xml);
    }
}