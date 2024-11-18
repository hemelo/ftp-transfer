package com.hemelo.connect.run;

import com.hemelo.connect.MainAux;
import com.hemelo.connect.dto.FileWrapper;
import com.hemelo.connect.enums.FileStatusLocal;
import com.hemelo.connect.enums.FileStatusRemoto;
import com.hemelo.connect.infra.FTPClient;
import com.hemelo.connect.utils.FiltrarArquivo;
import org.apache.commons.net.ftp.FTPFile;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * Retorna uma funcao responsavel por procurar por novos arquivos
 */
public class ArquivosFtpVerifierRunnable implements Runnable {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArquivosFtpVerifierRunnable.class);

    @Override
    public void run() {

        // Funcao a ser executada ao finalizar a busca por novos arquivos
        Runnable onFinish = () -> {
            synchronized (MainAux.isProcurandoArquivos) {
                MainAux.isProcurandoArquivos.set(false);
                MainAux.isProcurandoArquivos.notifyAll();
            }

            // Atualiza o status
            synchronized (MainAux.status) {
                MainAux.status.set("");
                MainAux.status.notifyAll();
            }
        };

        // Verifica se já está procurando arquivos
        synchronized (MainAux.isProcurandoArquivos) {
            if (MainAux.isProcurandoArquivos.get()) {
                logger.debug("Já está procurando arquivos");
                return;
            }

            MainAux.isProcurandoArquivos.set(true);
            MainAux.isProcurandoArquivos.notifyAll();
        }

        // Verifica se está resetando dados e aguarda a finalizacao
        synchronized (MainAux.isResettingData) {

            while (MainAux.isResettingData.get()) {
                try {
                    logger.debug("Aguardando a finalizacao do processo de resetar dados para procurar novos arquivos");
                    MainAux.isResettingData.wait();
                } catch (InterruptedException e) {
                    logger.error("Erro ao aguardar a finalizacao da thread de resetar dados", e);
                }
            }
        }

        // Atualiza o status
        synchronized (MainAux.status) {
            MainAux.status.set("Procurando novos arquivos...");
            MainAux.status.notifyAll();
        }

        // Inicia a busca por novos arquivos

        MainAux.ultimaBuscaArquivosInstant = Instant.now();

        logger.debug("Procurando por novos arquivos...");

        try {

            if (!FTPClient.moveToWorkBaseDirectory()) {
                logger.error("Erro ao mudar para a pasta de trabalho");

                synchronized (MainAux.isProcurandoArquivos) {
                    MainAux.isProcurandoArquivos.set(false);
                    MainAux.isProcurandoArquivos.notifyAll();
                }

                return;
            }

            FTPFile[] diretorios = FTPClient.getInstance().listDirectories();

            if (diretorios == null || diretorios.length == 0) {
                logger.error("Não foram encontrados pastas no FTP em " + FTPClient.getInstance().printWorkingDirectory());

                onFinish.run();

                return;
            }

            // Itera sobre os diretorios para procurar por arquivos
            for (FTPFile diretorio : diretorios) {

                if (!diretorio.isDirectory()) continue;

                if (!FTPClient.changeWorkingDirectory(diretorio.getName())) {
                    logger.error(String.format("Erro ao mudar de diretorio para %s", diretorio.getName()));
                    continue;
                }

                for (FTPFile file : FTPClient.getInstance().listFiles()) {

                    Optional<FileWrapper> fileBaseOpt = MainAux.arquivosParaEnviar.stream().filter(a -> a.equals(file)).findFirst();

                    if (fileBaseOpt.isPresent()) {

                        if (fileBaseOpt.get().getFtpFile().getSize() != file.getSize()) { // Se o tamanho do arquivo for diferente, provavelmente está em transferência
                            fileBaseOpt.get().setStatusFtp(FileStatusRemoto.EM_TRANSFERENCIA);
                        } else { // Se o tamanho do arquivo for igual, então o arquivo foi transferido
                            fileBaseOpt.get().setStatusFtp(FileStatusRemoto.TRANSFERIDO);
                        }

                        fileBaseOpt.get().setFtpFile(file);
                        continue;
                    }

                    FileWrapper fileBase = new FileWrapper(file);
                    fileBase.setParent(diretorio.getName());
                    fileBase.setFtpCaminhoBase(FTPClient.getInstance().printWorkingDirectory());
                    fileBase.setStatusFtp(FileStatusRemoto.NECESSARIO_VERIFICACAO);
                    fileBase.setStatusLocal(FileStatusLocal.INDISPONIVEL);
                    fileBase.setIsCredenciado(FiltrarArquivo.validarCredenciado(fileBase.getNomeArquivo(), fileBase.getParent()));
                    MainAux.arquivosParaEnviar.add(fileBase);
                }

                FTPClient.getInstance().changeToParentDirectory();
            }

        } catch (Exception e) {
            logger.error("Erro ao procurar por novos arquivos", e);
        }

        // Remove arquivos que não são credenciados
        MainAux.arquivosParaEnviar.removeIf(a -> !a.isCredenciado());

        // Itera sobre os arquivos para verificar se algum arquivo não foi encontrado na busca atual
        // Se não foi encontrado, então o arquivo foi removido do servidor FTP ou o programa falhou ao encontrar o arquivo
        final List<FileWrapper> arquivosPerdidosStream = MainAux.arquivosParaEnviar.stream().filter(a -> a.getFtpLatestUpdate().isBefore(MainAux.ultimaBuscaArquivosInstant) && a.getStatusFtp() != FileStatusRemoto.REFERENCIA_PERDIDA && a.getStatusFtp() != FileStatusRemoto.DELETADO).toList();
        arquivosPerdidosStream.forEach(a -> a.setStatusFtp(FileStatusRemoto.REFERENCIA_PERDIDA));

        // Verifica se existem arquivos que foram encontrados previamente, mas não foram encontrados agora
        if (!arquivosPerdidosStream.isEmpty()) {
            logger.info(String.format("%d arquivos foram perdidos: [%s]", arquivosPerdidosStream.size(), arquivosPerdidosStream.stream().map(MainAux::getStrArquivoSimplificado).collect(Collectors.joining(", "))));
        }

        final List<FileWrapper> arquivosNovosStream = MainAux.arquivosParaEnviar.stream().filter(a -> a.getFindedAt().isAfter(MainAux.ultimaBuscaArquivosInstant) || a.getFindedAt().equals(MainAux.ultimaBuscaArquivosInstant)).toList();

        // Verifica se não foram encontrados arquivos
        if (arquivosNovosStream.isEmpty()) {
            logger.warn("Nenhum novo arquivo encontrado");
        } else {
            logger.info(String.format("%d novos arquivos foram encontrados: [%s]", arquivosNovosStream.size(), arquivosNovosStream.stream().map(MainAux::getStrArquivoSimplificado).collect(Collectors.joining(", "))));
        }

        onFinish.run();

        logger.debug("Procura por novos arquivos finalizada");
    }

}
