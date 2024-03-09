using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("referencias")]
    public class XmlReferencias
    {
        [XmlElement("referencia")]
        public XmlReferencia referencia { get; set; }
    }

    [XmlRoot("referencia")]
    public class XmlReferencia
    {
        [XmlElement("idnumeracion")]
        public string idnumeracion { get; set; }

        [XmlElement("numero")]
        public string numero { get; set; }
    }
}
