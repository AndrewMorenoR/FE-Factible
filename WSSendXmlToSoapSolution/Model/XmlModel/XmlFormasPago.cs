using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("formaspago")]
    public class XmlFormasPago
    {
        [XmlElement("formapago")]
        public XmlFormaPago FormaPago { get; set; }
    }

    [XmlRoot("formapago")]
    public class XmlFormaPago
    {
        [XmlElement("idmetodopago")]
        public string idmetodopago { get; set; }

        [XmlElement("idmediopago")]
        public string idmediopago { get; set; }

        [XmlElement("fechavencimiento")]
        public string fechavencimiento { get; set; }

        [XmlElement("identificador")]
        public string identificador { get; set; }

        [XmlElement("dias")]
        public string dias { get; set; }
    }
}
