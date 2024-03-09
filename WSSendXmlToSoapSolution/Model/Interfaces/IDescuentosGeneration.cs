using Model.XmlModel;
using System.Collections.Generic;

namespace Model.Data
{
    public interface IDescuentosGeneration
    {
        /// <summary>
		/// Genera un listado de objetos del tipo XmlCargo tomando como entrada la respuesta del query de Descuentos
		/// </summary>
		/// <returns> Devuelve un listado de objetos XmlCargo los cuales son los descuentos asignados a los documentos a procesar  </returns>
        List<XmlCargo> GenerateDescuentosList();
    }
}