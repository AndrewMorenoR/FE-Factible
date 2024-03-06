using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("otroautorizado")]
    public class XmlOtroAutorizado
    {
        [XmlElement("idtipopersona")]
        public string idtipopersona { get; set; }

        [XmlElement("idactividadeconomica")]
        public string idactividadeconomica { get; set; }

        [XmlElement("nombrecomercial")]
        public string nombrecomercial { get; set; }

        [XmlElement("idciudad")]
        public string idciudad { get; set; }

        [XmlElement("direccion")]
        public string direccion { get; set; }

        [XmlElement("codigopostal")]
        public string codigopostal { get; set; }

        [XmlElement("nombres")]
        public string nombres { get; set; }

        [XmlElement("apellidos")]
        public string apellidos { get; set; }

        [XmlElement("idtipodocumentoidentidad")]
        public string idtipodocumentoidentidad { get; set; }

        [XmlElement("identificacion")]
        public string identificacion { get; set; }

        [XmlElement("digitoverificacion")]
        public string digitoverificacion { get; set; }

        [XmlElement("idtiporegimen")]
        public string idtiporegimen { get; set; }

        [XmlElement("direccionfiscal")]
        public XmlDireccionFiscal direccionfiscal { get; set; }

        [XmlElement("matriculamercantil")]
        public string matriculamercantil { get; set; }

        [XmlElement("emailcontacto")]
        public string emailcontacto { get; set; }

        [XmlElement("emailentrega")]
        public string emailentrega { get; set; }

        [XmlElement("telefono")]
        public string telefono { get; set; }

        [XmlElement("obligaciones")]
        public string obligaciones { get; set; }
    }
}
