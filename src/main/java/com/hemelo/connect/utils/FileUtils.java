package com.hemelo.connect.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class FileUtils {

    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    /**
     * Calcula o hash 256 de um arquivo
     *
     * @param filePath Caminho do arquivo
     * @return Hash do arquivo
     */
    public static String calculateFileHash256(String filePath) {
        return calculateFileHash(filePath, "SHA-256");
    }

    /**
     * Calcula o hash 1 de um arquivo
     *
     * @param filePath Caminho do arquivo
     * @return Hash do arquivo
     */
    public static String calculateFileHash1(String filePath) {
        return calculateFileHash(filePath, "SHA-1");
    }

    /**
     * Calcula o hash de um arquivo
     *
     * @param filePath  Caminho do arquivo
     * @param algorithm Algoritmo de hash
     * @return Hash do arquivo
     */
    public static String calculateFileHash(String filePath, String algorithm) {
        try {
            byte[] input = getFileContent(filePath, Charset.defaultCharset()).getBytes();
            MessageDigest md = MessageDigest.getInstance(algorithm);
            md.update(input, 0, input.length);
            byte[] hashBytes = md.digest();

            StringBuilder sb = new StringBuilder();

            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }

            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            logger.debug("Erro ao calcular o hash do arquivo " + Paths.get(filePath).getFileName(), e);
            return null;
        }
    }

    /**
     * Retorna o conteúdo de um arquivo
     *
     * @param filename Caminho do arquivo
     * @param charset  Charset do arquivo
     * @return
     */
    public static String getFileContent(String filename, Charset charset) {

        String content = null;
        File file = new File(filename);

        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] data = new byte[(int) file.length()];
            fis.read(data);
            content = new String(data, charset);
        } catch (IOException ex) {
            logger.error(ex.getMessage());
            logger.debug(Arrays.toString(ex.getStackTrace()));
        }

        return content;
    }

    /**
     * Cria um diretório se ele não existir
     * @param diretorio
     * @return
     */
    public static boolean createDirectoryIfNotExists(String diretorio) {

        File file = new File(diretorio);

        if (!file.exists()) {
            logger.trace("Criando diretório: " + diretorio);
            Boolean result = file.mkdirs();

            if (result) {
                logger.info("Diretório " + diretorio + " criado com sucesso.");
            } else {
                logger.error("Erro ao criar diretório.");
            }

            return result;
        }

        return true;
    }

    public static long getFolderSize(File folder) {
        long size = 0;
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    size += file.length();
                } else if (file.isDirectory()) {
                    size += getFolderSize(file);
                }
            }
        }
        return size;
    }

    public static void zipFile(String file, String zipName) throws IOException {
        FileOutputStream fos = new FileOutputStream(zipName + ".zip");
        ZipOutputStream zipOut = new ZipOutputStream(fos);

        File fileToZip = new File(file);
        zipFile(fileToZip, fileToZip.getName(), zipOut);
        zipOut.close();
        fos.close();
    }

    public static void zipFile(File fileToZip, String fileName, ZipOutputStream zipOut) throws IOException {
        if (fileToZip.isHidden()) {
            return;
        }
        if (fileToZip.isDirectory()) {
            if (fileName.endsWith("/")) {
                zipOut.putNextEntry(new ZipEntry(fileName));
                zipOut.closeEntry();
            } else {
                zipOut.putNextEntry(new ZipEntry(fileName + "/"));
                zipOut.closeEntry();
            }
            File[] children = fileToZip.listFiles();
            for (File childFile : children) {
                zipFile(childFile, fileName + "/" + childFile.getName(), zipOut);
            }
            return;
        }
        FileInputStream fis = new FileInputStream(fileToZip);
        ZipEntry zipEntry = new ZipEntry(fileName);
        zipOut.putNextEntry(zipEntry);
        byte[] bytes = new byte[1024];
        int length;
        while ((length = fis.read(bytes)) >= 0) {
            zipOut.write(bytes, 0, length);
        }
        fis.close();
    }

    public static boolean tryOldifyFile(String filePath) throws IOException {
        return addSuffixToFile(filePath, "old");
    }

    /**
     * Verifica se existe um arquivo com o caminho informado, se sim renomeia-o arquivo para o sufixo informado
     * @param filePath Caminho do arquivo
     * @throws IOException Em caso de erro ao renomear o arquivo
     *
     * @return true se o arquivo foi renomeado, false caso contrário
     */
    public static boolean addSuffixToFile(String filePath, String suffix) throws IOException {
        Path file = Path.of(filePath);

        // Verifica se o arquivo existe
        if (Files.exists(file)) {
            String newFileName = file.getFileName().toString() + "." + suffix;
            Path renamedFile = file.resolveSibling(newFileName);

            // Incrementa o sufixo se o arquivo com suffix já existir
            int counter = 1;
            while (Files.exists(renamedFile)) {
                newFileName = file.getFileName().toString() + "." + suffix + "." + counter;
                renamedFile = file.resolveSibling(newFileName);
                counter++;
            }

            // Renomeia o arquivo
            Files.move(file, renamedFile, StandardCopyOption.REPLACE_EXISTING);
            logger.info("Arquivo renomeado para: \"" + renamedFile.getFileName() + "\"");

            return true;
        }

        return false;
    }

    /**
     * Retorna a extensão de um arquivo
     * @param fileName Nome do arquivo
     * @return Extensão do arquivo
     */
    public static String getFileExtension(String fileName) {
        int index = fileName.lastIndexOf('.');
        return index == -1 ? "" : fileName.substring(index + 1);
    }

    /**
     * Busca arquivos relacionados a um arquivo
     * @param directoryPath Caminho do diretório
     * @param fileName Nome do arquivo
     * @return
     * @throws IOException Em caso de erro ao buscar os arquivos
     */
    public static List<Path> findRelatedFiles(String directoryPath, String fileName) throws IOException {
        List<Path> relatedFiles = new ArrayList<>();
        Path dir = Paths.get(directoryPath);

        // Verifica se o diretório existe
        if (Files.exists(dir) && Files.isDirectory(dir)) {
            String baseName = fileName.substring(0, fileName.lastIndexOf('.'));

            // Busca os arquivos correspondentes
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, baseName + "*")) {
                for (Path entry : stream) {
                    relatedFiles.add(entry);
                }
            }
        } else {
            logger.trace("Diretório não encontrado: " + directoryPath);
        }

        return relatedFiles;
    }
}
