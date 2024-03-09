using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("constanciarecibido")]
    public class XmlConstanciaRecibido
    {
        [XmlElement("codigo")]
        public string codigo { get; set; }

        [XmlElement("fechageneracion")]
        public string fechageneracion { get; set; }

        [XmlElement("base64")]
        public string base64 { get; set; }

        [XmlElement("nombrearchivo")]
        public string nombrearchivo { get; set; }
    }
}
