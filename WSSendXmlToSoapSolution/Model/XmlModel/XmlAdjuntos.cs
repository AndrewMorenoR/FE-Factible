using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("adjuntos")]
    public class XmlAdjuntos
    {
        [XmlElement("adjunto")]
        public XmlAdjunto adjunto { get; set; }
    }

    [XmlRoot("adjunto")]
    public class XmlAdjunto
    {
        [XmlElement("codigo")]
        public string codigo { get; set; }

        [XmlElement("idtipoadjunto")]
        public string idtipoadjunto { get; set; }

        [XmlElement("cufe")]
        public string cufe { get; set; }

        [XmlElement("nombrearchivo")]
        public string nombrearchivo { get; set; }

        [XmlElement("base64")]
        public string base64 { get; set; }

        [XmlElement("fechageneracion")]
        public string fechageneracion { get; set; }
    }
}
