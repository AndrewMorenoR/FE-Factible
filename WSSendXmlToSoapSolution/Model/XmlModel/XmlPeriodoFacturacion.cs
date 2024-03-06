using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("periodofacturacion")]
    public class XmlPeriodoFacturacion
    {
        [XmlElement("fechainicial")]
        public string fechainicial { get; set; }

        [XmlElement("fechafinal")]
        public string fechafinal { get; set; }
    }
}
