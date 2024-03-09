using Model.XmlModel;
using System.Collections.Generic;

namespace Model.Data
{
    public interface IAnticiposGeneration
    {
        /// <summary>
		/// Genera un listado de objetos del tipo XmlAnticipo tomando como entrada la respuesta del query
		/// </summary>
		/// <returns> Devuelve un listado de objetos XmlAnticipo los cuales son los anticipos asignados a los documentos a procesar  </returns>
        List<XmlAnticipo> GenerateAnticiposList();
    }
}