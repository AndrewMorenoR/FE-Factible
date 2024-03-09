using Business;
using Business.Soap;
using Log;
using Model.Data;
using Model.Query;
using Ninject;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestProject
{
    class Program
    {
        static void Main(string[] args)
        {
            //Se genera una nueva instacia del tipo StandardKernel para la injeccion de dependencias
            var kernel = new StandardKernel(new DependencyInjection());
            var log = kernel.Get<EventLogStore>();
            var soap = kernel.Get<SoapProcess>();
            var process = kernel.Get<XmlProcess>();
            var general = kernel.Get<GeneralDataGeneration>();
            var query = kernel.Get<DbQuery>();

            GeneralProcess p = new GeneralProcess(log, soap, process, general, query);
            p.MainProcess();
            
    }
    }
}
