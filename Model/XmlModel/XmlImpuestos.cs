using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("impuestos")]
    public class XmlImpuestos
    {
        [XmlElement("impuesto")]
        public List<XmlImpuesto> impuesto { get; set; }
    }

    [XmlRoot("impuesto")]
    public class XmlImpuesto
    {
        [XmlIgnore]
        public string DOCNUM { get; set; }

        [XmlElement("idimpuesto")]
        public string idimpuesto { get; set; }

        [XmlElement("base")]
        public string baseImp { get; set; }

        [XmlElement("factor")]
        public string factor { get; set; }

        [XmlElement("estarifaunitaria")]
        public string estarifaunitaria { get; set; }

        [XmlElement("valor")]
        public string valor { get; set; }
    }
}
