using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("totales")]
    public class XmlTotales
    {
        [XmlElement("totalbruto")]
        public string totalbruto { get; set; }

        [XmlElement("baseimponible")]
        public string baseimponible { get; set; }

        [XmlElement("totalbrutoconimpuestos")]
        public string totalbrutoconimpuestos { get; set; }

        [XmlElement("totaldescuento")]
        public string totaldescuento { get; set; }

        [XmlElement("totalcargos")]
        public string totalcargos { get; set; }

        [XmlElement("totalanticipos")]
        public string totalanticipos { get; set; }

        [XmlElement("totalapagar")]
        public string totalapagar { get; set; }

        [XmlElement("payableroundingamount")]
        public string payableroundingamount { get; set; }
    }
}
