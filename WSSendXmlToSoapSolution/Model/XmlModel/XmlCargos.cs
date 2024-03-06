using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("cargos")]
    public class XmlCargos
    {
        [XmlElement("cargo")]
        public List<XmlCargo> cargo { get; set; }
    }

    [XmlRoot("cargo")]
    public class XmlCargo
    {
        [XmlIgnore]
        public string DOCNUM { get; set; }

        [XmlElement("idconcepto")]
        public string idconcepto { get; set; }

        [XmlElement("escargo")]
        public string escargo { get; set; }

        [XmlElement("descripcion")]
        public string descripcion { get; set; }

        [XmlElement("porcentaje")]
        public string porcentaje { get; set; }

        [XmlElement("base")]
        public string baseCargo { get; set; }

        [XmlElement("valor")]
        public string valor { get; set; }
    }
}
