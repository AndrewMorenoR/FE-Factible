using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("anticipos")]
    public class XmlAnticipos
    {
        [XmlElement("anticipo")]
        public List<XmlAnticipo> anticipo { get; set; }
    }

    [XmlRoot("anticipo")]
    public class XmlAnticipo
    {
        [XmlIgnore]
        public string DOCNUM { get; set; }

        [XmlElement("identificador")]
        public string identificador { get; set; }

        [XmlElement("valor")]
        public string valor { get; set; }

        [XmlElement("fecha")]
        public string fecha { get; set; }
    }

}
