using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("root")]
    public class MainXmlModel
    {
        [XmlElement("documento")]
        public Xmldocumento doc { get; set; }
    }
}
