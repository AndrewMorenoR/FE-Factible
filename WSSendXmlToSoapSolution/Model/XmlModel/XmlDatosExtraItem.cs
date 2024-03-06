using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("datosextra")]
    public class XmlDatosExtraItem
    {
        [XmlElement("datoextra")]
        public XmlDatoExtraItem datoextra { get; set; }
    }

    [XmlRoot("datoextra")]
    public class XmlDatoExtraItem
    {
        [XmlElement("clave")]
        public string clave { get; set; }

        [XmlElement("valor")]
        public string valor { get; set; }
    }
}
