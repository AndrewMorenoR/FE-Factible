using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("items")]
    public class XmlItems
    {
        [XmlElement("item")]
        public List<XmlItem> item { get; set; }
    }

    [XmlRoot("item")]
    public class XmlItem
    {
        [XmlIgnore]
        public string DOCNUM { get; set; }

        [XmlElement("codigos")]
        public XmlCodigos codigos { get; set; }

        [XmlElement("descripcion")]
        public string descripcion { get; set; }

        [XmlElement("notas")]
        public string notas { get; set; }

        [XmlElement("cantidad")]
        public string cantidad { get; set; }

        [XmlElement("cantidadporempaque")]
        public string cantidadporempaque { get; set; }

        [XmlElement("preciounitario")]
        public string preciounitario { get; set; }

        [XmlElement("unidaddemedida")]
        public string unidaddemedida { get; set; }

        [XmlElement("marca")]
        public string marca { get; set; }

        [XmlElement("modelo")]
        public string modelo { get; set; }

        [XmlElement("codigovendedor")]
        public string codigovendedor { get; set; }

        [XmlElement("subcodigovendedor")]
        public string subcodigovendedor { get; set; }

        [XmlElement("idmandante")]
        public string idmandante { get; set; }

        [XmlElement("regalo")]
        public string regalo { get; set; }

        [XmlElement("totalitem")]
        public string totalitem { get; set; }

        [XmlElement("fechacompra")]
        public string fechacompra { get; set; }

        [XmlElement("formageneraciontransmision")]
        public string formageneraciontransmision { get; set; }

        [XmlElement("cargos")]
        public XmlCargos cargos { get; set; }

        [XmlElement("impuestos")]
        public XmlImpuestos impuestos { get; set; }

        [XmlElement("datosextra")]
        public XmlDatosExtraItem datosextra { get; set; }
    }

    [XmlRoot("codigos")]
    public class XmlCodigos
    {
        [XmlElement("estandar")]
        public XmlEstandar estandar { get; set; }
    }

    [XmlRoot("estandar")]
    public class XmlEstandar
    {
        [XmlElement("idestandar")]
        public string idestandar { get; set; }

        [XmlElement("nombreestandar")]
        public string nombreestandar { get; set; }

        [XmlElement("codigo")]
        public string codigo { get; set; }
    }
}
