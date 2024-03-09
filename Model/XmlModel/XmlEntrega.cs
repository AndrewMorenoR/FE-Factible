using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("entrega")]
    public class XmlEntrega
    {
        [XmlElement("fecha")]
        public string fecha { get; set; }

        [XmlElement("idciudad")]
        public string idciudad { get; set; }

        [XmlElement("direccion")]
        public string direccion { get; set; }

        [XmlElement("codigopostal")]
        public string codigopostal { get; set; }

        [XmlElement("transportador")]
        public XmlTransportador transportador { get; set; }

        [XmlElement("metododepago")]
        public string metododepago { get; set; }

        [XmlElement("condicionesdeentrega")]
        public string condicionesdeentrega { get; set; }
    }
}
