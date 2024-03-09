using Model.XmlModel;
using System.Collections.Generic;

namespace Model.Data
{
    public interface IItemsGeneration
    {
        /// <summary>
		/// Genera un listado de objetos del tipo XmlItem tomando como entrada la respuesta del query
		/// </summary>
		/// <returns> Devuelve un listado de objetos XmlItem los cuales son los Items asignados a los documentos a procesar  </returns>
        List<XmlItem> GenerateItemsList();
    }
}