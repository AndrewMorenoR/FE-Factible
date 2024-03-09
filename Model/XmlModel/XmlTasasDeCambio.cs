using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("tasasdecambio")]
    public class XmlTasasdecambio
    {
        [XmlElement("tasadecambio")]
        public XmlTasadecambio tasadecambio { get; set; }
    }

    [XmlRoot("tasadecambio")]
    public class XmlTasadecambio
    {
        [XmlElement("fecha")]
        public string fecha { get; set; }

        [XmlElement("moneda")]
        public string moneda { get; set; }

        [XmlElement("cambiominimo")]
        public string cambiominimo { get; set; }

        [XmlElement("tasarepresentativa")]
        public string tasarepresentativa { get; set; }
    }
}
