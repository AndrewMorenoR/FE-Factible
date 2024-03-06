using Business;
using Log;
using Ninject;
using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace WSSendXmlToSoap
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
            //Se genera una nueva instacia del tipo StandardKernel para la injeccion de dependencias
            var kernel = new StandardKernel(new DependencyInjection());
            var generalP = kernel.Get<GeneralProcess>();
            var log = kernel.Get<EventLogStore>();
            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[]
            {
                new Process(log, generalP)
            };
            ServiceBase.Run(ServicesToRun);
        }
    }
}
