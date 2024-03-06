using Model.XmlModel;
using System.Collections.Generic;

namespace Model.Data
{
    public interface IDatosExtraGeneration
    {
        /// <summary>
		/// Genera un listado de objetos del tipo XmlImpuesto tomando como entrada la respuesta del query
		/// </summary>
		/// <returns> Devuelve un listado de objetos XmlImpuesto los cuales son los Impuestos asignados a los documentos a procesar  </returns>
        List<XmlDatoExtra> GenerateDatosExtraList();
    }
}
