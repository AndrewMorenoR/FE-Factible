using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("documento")]
    public class Xmldocumento
    {
        [XmlIgnore]
        public string DOCNUM { get; set; }

        [XmlElement("idnumeracion")]
        public string idnumeracion { get; set; }

        [XmlElement("numero")]
        public string numero { get; set; }

        [XmlElement("idambiente")]
        public string idambiente { get; set; }

        [XmlElement("idreporte")]
        public string idreporte { get; set; }

        [XmlElement("fechadocumento")]
        public string fechadocumento { get; set; }

        [XmlElement("fechavencimiento")]
        public string fechavencimiento { get; set; }

        [XmlElement("tipofactura")]
        public string tipofactura { get; set; }

        [XmlElement("tipooperacion")]
        public string tipooperacion { get; set; }

        [XmlElement("notas")]
        public string notas { get; set; }

        [XmlElement("fechaimpuestos")]
        public string fechaimpuestos { get; set; }

        [XmlElement("moneda")]
        public string moneda { get; set; }

        [XmlElement("cufe")]
        public string cufe { get; set; }

        [XmlElement("idconceptonota")]
        public string idconceptonota { get; set; }

        [XmlElement("referencias")]
        public XmlReferencias referencias { get; set; }

        [XmlElement("tasasdecambio")]
        public XmlTasasdecambio tasasdecambio { get; set; }

        [XmlElement("periodofacturacion")]
        public XmlPeriodoFacturacion periodofacturacion { get; set; }



        [XmlIgnore]
        [XmlElement("correoscopia")]
        public XmlCorreosCopia correoscopia { get; set; }

        [XmlElement("formaspago")]
        public XmlFormasPago formaspago { get; set; }






        [XmlIgnore]
        [XmlElement("entrega")]
        public XmlEntrega entrega { get; set; }

        [XmlElement("facturador")]
        public XmlFacturador facturador { get; set; }

        [XmlElement("adquiriente")]
        public XmlAdquiriente adquiriente { get; set; }
        //borrar esto
        [XmlElement("otroautorizado")]
        public XmlOtroAutorizado otroautorizado { get; set; }
        //hasta aqui
        [XmlElement("impuestos")]
        public XmlImpuestos impuestos { get; set; }

        [XmlElement("anticipos")]
        public XmlAnticipos anticipos { get; set; }

        [XmlElement("cargos")]
        public XmlCargos cargos { get; set; }

        [XmlElement("totales")]
        public XmlTotales totales { get; set; }

        [XmlElement("items")]
        public XmlItems items { get; set; }
        //borrar desde aqui
        [XmlElement("adjuntos")]
        public XmlAdjuntos adjuntos { get; set; }

        [XmlElement("ordendecompra")]
        public XmlOrdenDeCompra ordendecompra { get; set; }

        [XmlElement("constanciarecibido")]
        public XmlConstanciaRecibido constanciarecibido { get; set; }

        [XmlElement("ordendedespacho")]
        public XmlOrdenDeDespacho ordendedespacho { get; set; }

        [XmlElement("datosextra")]
        public XmlDatosExtra datosextra { get; set; }
    }
}
