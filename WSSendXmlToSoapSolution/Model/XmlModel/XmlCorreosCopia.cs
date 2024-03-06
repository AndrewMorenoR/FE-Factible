using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Model.XmlModel
{
    [XmlRoot("correoscopia")]
    public class XmlCorreosCopia
    {
        [XmlElement("correocopia")]
        public string correocopia { get; set; }
    }
}
