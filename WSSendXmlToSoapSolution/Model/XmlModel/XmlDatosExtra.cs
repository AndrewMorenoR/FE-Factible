using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("datosextra")]
    public class XmlDatosExtra
    {
        [XmlElement("datoextra")]
        public List<XmlDatoExtra> datoextra { get; set; }
    }

    [XmlRoot("datoextra")]
    public class XmlDatoExtra
    {
        [XmlIgnore]
        public string DOCNUM { get; set; }

        [XmlElement("tipo")]
        public string tipo { get; set; }

        [XmlElement("clave")]
        public string clave { get; set; }

        [XmlElement("valor")]
        public string valor { get; set; }

        //       [XmlElement("valor")]
        //       public XmlValor valor { get; set; }
    }

 /*   [XmlRoot("valor")]
    public class XmlValor
    {
        [XmlElement("columnas")]
        public XmlColumnas columnas { get; set; }

        [XmlElement("filas")]
        public XmlFilas filas { get; set; }
    }

    [XmlRoot("columnas")]
    public class XmlColumnas
    {
        [XmlElement("columna")]
        public string columna { get; set; }
    }

    [XmlRoot("filas")]
    public class XmlFilas
    {
        [XmlElement("fila")]
        public XmlFila fila { get; set; }
    }

    [XmlRoot("fila")]
    public class XmlFila
    {
        [XmlElement("celda")]
        public string celda { get; set; }
    }
    */
}
