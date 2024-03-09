using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Model
{
    //Estructura de la respuesta generada por el servicio Web
    public class SoapResponse
    {
        public string software { get; set; }

        public bool success { get; set; }

        public string msg { get; set; }
        public string metodo { get; set; }
        public string time { get; set; }
    }

    public class SoapResponseString : SoapResponse
    {
        public DataSoapResponseString data { get; set; }
    }

    public class SoapResponseObject : SoapResponse
    {
        public List<DataSoapResponseObject> data { get; set; }
    }


    public class DataSoapResponseString
    {
        public string salida { get; set; }
    }

    public class DataSoapResponseObject : SoapResponse
    {
        public string indice { get; set; }
        public string idtipodocumentoidentidad { get; set; }
        public string identificacion { get; set; }
        public string totalImporteBruto { get; set; }
        public string totalBaseImponible { get; set; }
        public string TotalPagado { get; set; }
        public string idDocumentoElectronico { get; set; }
        public string numero { get; set; }
        public string idnumeracion { get; set; }
        public string cufe { get; set; }
        public string qr_data { get; set; }
        public string qr { get; set; }
        public string errorcufe { get; set; }
        public string cuferecibido { get; set; }
        public string idExterno { get; set; }
        public string enviadoUsandoSealMail { get; set; }
    }
}
